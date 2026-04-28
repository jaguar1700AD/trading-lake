"""Generate synthetic trades, quotes, and corporate events in S3."""

import argparse
import csv
import gzip
import io
import json
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
import numpy as np

EXCHANGES = ["NYSE", "NASDAQ", "ARCA"]
SYMBOLS = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "NVDA", "META", "JPM", "BAC", "GS",
           "ORCL", "CRM", "AMD", "INTC", "NFLX", "DIS", "WMT", "XOM", "CVX", "PFE"]

# Active hours are UTC.
MARKET_HOURS = list(range(9, 16))

INITIAL_PRICES = {
    "AAPL": 185.0, "MSFT": 380.0, "AMZN": 175.0, "GOOGL": 140.0, "TSLA": 250.0,
    "NVDA": 480.0, "META": 500.0, "JPM": 195.0,  "BAC": 37.0,   "GS": 395.0,
    "ORCL": 130.0, "CRM": 280.0,  "AMD": 165.0,  "INTC": 32.0,  "NFLX": 620.0,
    "DIS":  95.0,  "WMT":  78.0,  "XOM": 115.0,  "CVX": 155.0,  "PFE":  28.0,
}
SPREAD_BPS = 5
TICK_INTERVAL_SEC = 0.3

# Keep this order in sync with ingest_bronze_events.py.
EVENT_CSV_HEADER = [
    "event_id", "symbol", "event_type",
    "event_date", "event_start", "event_end", "description",
]

EVENT_TYPES = ["EARNINGS", "DIVIDEND", "SPLIT", "GUIDANCE", "BUYBACK"]

EVENT_DESCRIPTIONS = {
    "EARNINGS": "Quarterly earnings release",
    "DIVIDEND": "Dividend declaration",
    "SPLIT":    "Stock split announcement",
    "GUIDANCE": "Forward guidance update",
    "BUYBACK":  "Share buyback program",
}

# Keep event windows inside the generated trading day.
EVENT_START_EARLIEST_HOUR   = 10
EVENT_START_EARLIEST_MINUTE = 30
EVENT_START_LATEST_HOUR     = 15
EVENT_START_LATEST_MINUTE   = 0

EVENTS_PER_DAY_MEAN = 3


def _next_price(prev_price: float, sigma_per_tick: float = 0.0003) -> float:
    """Log-normal price step."""
    return round(prev_price * np.exp(np.random.normal(0, sigma_per_tick)), 4)


def _side() -> str:
    """Return BUY, SELL, or MID with realistic distribution."""
    r = np.random.random()
    if r < 0.45:
        return "BUY"
    elif r < 0.90:
        return "SELL"
    else:
        return "MID"


def generate_hour(
    exchange: str,
    symbol: str,
    trade_date: date,
    hour: int,
    initial_price: float,
    rng: np.random.Generator,
) -> tuple[list[dict], list[dict], float]:
    """Simulate one hour for an exchange/symbol pair."""
    trades: list[dict] = []
    quotes: list[dict] = []

    start_offset = 30 * 60 if hour == 9 else 0
    t = datetime(trade_date.year, trade_date.month, trade_date.day,
                 hour, 0, 0, tzinfo=timezone.utc)
    t = t + timedelta(seconds=start_offset)
    end_t = datetime(trade_date.year, trade_date.month, trade_date.day,
                     hour + 1, 0, 0, tzinfo=timezone.utc)

    price = initial_price
    prev_spread = round(price * SPREAD_BPS / 10_000, 4)
    prev_bid = round(price - prev_spread / 2, 4)
    prev_ask = round(price + prev_spread / 2, 4)
    seq = int(t.timestamp() * 1000) % (10 ** 9)

    while t < end_t:
        ts_str = t.strftime("%Y-%m-%dT%H:%M:%S.%f")

        # Trades execute against the prior quote to avoid lookahead.
        if rng.random() < 0.6:
            side = _side()
            if side == "BUY":
                trade_price = prev_ask
            elif side == "SELL":
                trade_price = prev_bid
            else:
                trade_price = round((prev_bid + prev_ask) / 2, 4)

            trades.append({
                "exchange":     exchange,
                "symbol":       symbol,
                "exchange_ts":  ts_str,
                "price":        trade_price,
                "size":         int(rng.integers(1, 1000)),
                "sequence_num": seq,
                "side":         side,
                "trade_id":     str(uuid.uuid4()),
            })

        price = _next_price(price)
        spread = round(price * SPREAD_BPS / 10_000, 4)
        bid = round(price - spread / 2, 4)
        ask = round(price + spread / 2, 4)

        quotes.append({
            "exchange":     exchange,
            "symbol":       symbol,
            "exchange_ts":  ts_str,
            "bid":          bid,
            "bid_size":     int(rng.integers(100, 5000)),
            "ask":          ask,
            "ask_size":     int(rng.integers(100, 5000)),
            "sequence_num": seq,
        })
        prev_bid, prev_ask = bid, ask

        seq += 1
        interval = rng.exponential(TICK_INTERVAL_SEC)
        t += timedelta(seconds=interval)

    return trades, quotes, price


def upload_records(records: list[dict], s3_client, bucket: str, s3_key: str) -> None:
    """Serialize records as JSONL, gzip, upload to S3."""
    if not records:
        return
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            gz.write((json.dumps(rec) + "\n").encode())
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=buf.read(),
                         ContentEncoding="gzip", ContentType="application/x-ndjson")
    print(f"  Uploaded {len(records):,} records → s3://{bucket}/{s3_key}")


def generate_events_for_day(
    trade_date: date,
    rng: np.random.Generator,
) -> list[dict]:
    """Generate corporate-event rows for a single calendar day."""
    n_events = int(rng.poisson(EVENTS_PER_DAY_MEAN))
    n_events = min(n_events, len(SYMBOLS))
    if n_events == 0:
        return []

    chosen_symbols = rng.choice(SYMBOLS, size=n_events, replace=False).tolist()

    earliest = datetime(
        trade_date.year, trade_date.month, trade_date.day,
        EVENT_START_EARLIEST_HOUR, EVENT_START_EARLIEST_MINUTE, 0,
        tzinfo=timezone.utc,
    )
    latest = datetime(
        trade_date.year, trade_date.month, trade_date.day,
        EVENT_START_LATEST_HOUR, EVENT_START_LATEST_MINUTE, 0,
        tzinfo=timezone.utc,
    )
    minutes_span = int((latest - earliest).total_seconds() // 60)

    events: list[dict] = []
    for i, symbol in enumerate(chosen_symbols):
        event_type = str(rng.choice(EVENT_TYPES))

        offset_min = int(rng.integers(0, minutes_span + 1))
        event_start_dt = earliest + timedelta(minutes=offset_min)

        duration_min = int(rng.integers(15, 61))
        event_end_dt = event_start_dt + timedelta(minutes=duration_min)

        event_id = (
            f"EVT-{trade_date.strftime('%Y%m%d')}-{symbol}-{event_type}-{i+1:02d}"
        )
        description = f"{symbol}: {EVENT_DESCRIPTIONS[event_type]}"

        events.append({
            "event_id":    event_id,
            "symbol":      symbol,
            "event_type":  event_type,
            "event_date":  trade_date.strftime("%Y-%m-%d"),
            "event_start": event_start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "event_end":   event_end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "description": description,
        })
    return events


def upload_events_csv(
    events: list[dict], s3_client, bucket: str, trade_date: date,
) -> None:
    """Serialize events as CSV and upload to S3."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(EVENT_CSV_HEADER)
    for ev in events:
        writer.writerow([ev[col] for col in EVENT_CSV_HEADER])

    body = buf.getvalue().encode("utf-8")
    s3_key = f"raw/events/events_{trade_date.strftime('%Y-%m-%d')}.csv"
    s3_client.put_object(
        Bucket=bucket, Key=s3_key, Body=body, ContentType="text/csv",
    )
    print(f"  Uploaded {len(events)} event(s) → s3://{bucket}/{s3_key}")


def generate_day(bucket: str, trade_date: date, s3_client) -> None:
    """Generate and upload all trades, quotes, and events for a single calendar day."""
    print(f"Generating data for {trade_date}")
    rng = np.random.default_rng(seed=int(trade_date.strftime("%Y%m%d")))
    # Keep event generation deterministic but independent from ticks.
    rng_events = np.random.default_rng(
        seed=int(trade_date.strftime("%Y%m%d")) ^ 0xE5E5E5E5
    )
    prices = dict(INITIAL_PRICES)

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for hour in MARKET_HOURS:
                trades, quotes, last_price = generate_hour(
                    exchange, symbol, trade_date, hour, prices[symbol], rng
                )
                prices[symbol] = last_price

                part_id = uuid.uuid4()
                dt_str = trade_date.strftime("%Y-%m-%d")
                hh_str = f"{hour:02d}"

                upload_records(
                    trades, s3_client, bucket,
                    f"raw/trades/dt={dt_str}/hh={hh_str}/part-{part_id}.jsonl.gz"
                )
                upload_records(
                    quotes, s3_client, bucket,
                    f"raw/quotes/dt={dt_str}/hh={hh_str}/part-{part_id}.jsonl.gz"
                )

    events = generate_events_for_day(trade_date, rng_events)
    upload_events_csv(events, s3_client, bucket, trade_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic trading data")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--start-date", default=None,
                        help="Start date YYYY-MM-DD (default: today)")
    parser.add_argument("--end-date", default=None,
                        help="End date YYYY-MM-DD inclusive (default: today)")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    today = date.today()
    start = date.fromisoformat(args.start_date) if args.start_date else today
    end = date.fromisoformat(args.end_date) if args.end_date else today

    s3 = boto3.client("s3", region_name=args.region)

    current = start
    while current <= end:
        if current.weekday() < 5:
            generate_day(args.bucket, current, s3)
        current += timedelta(days=1)

    print("Done.")


if __name__ == "__main__":
    main()
