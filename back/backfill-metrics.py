# backfill_metrics.py. rev:12Dec 2025 ver:1; Python bulk backfill for perp_metrics table using COPY batching, concurrency, and error handling.

import asyncio
import signal
import time
from datetime import datetime, timedelta

from back.api_utils import log_status, log_error
from back.dbsetup2 import DBManager

# Default retention days fallback
DEFAULT_RETENTION_DAYS = 20

# Configurations
SCRIPT_NAME = "backfill_metrics.py"
BUFFER_MS = 10 * 60 * 1000  # 10 minutes buffer for calc window
INSERT_CHUNK_SIZE = 6000
PARALLEL_SYMBOLS = 4
HEARTBEAT_INTERVAL_MS = 7000

# ===================================================================
# Helper Async sleep function
# ===================================================================
async def sleep(ms):
    await asyncio.sleep(ms / 1000)

# ===================================================================
# Calculate percent change, safe for nulls
# ===================================================================
def calculate_percent_change(current, previous):
    if previous in (None, 0):
        return None
    if current is None:
        return None
    try:
        change = ((current - previous) / abs(previous)) * 100
        return max(min(round(change, 3), 9999.999), -9999.999)
    except Exception:
        return None

# ===================================================================
# Fetch distinct symbols for backfill
# ===================================================================
async def fetch_distinct_symbols(db, min_ts):
    query = "SELECT DISTINCT(symbol) FROM perp_data WHERE ts >= %s;"
    rows = await db.execute_query(query, (min_ts,), fetch="all")
    return [r[0] for r in rows] if rows else []

# ===================================================================
# Fetch raw perp_data for symbol+exchange in time window
# ===================================================================
async def fetch_raw_data(db, symbol, exchange, start_ts, end_ts):
    base_query = """
        SELECT ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr,
               lql, lqs, rsi, tbv, tsv
        FROM perp_data
        WHERE symbol = %s AND exchange = %s AND ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """
    buffered_start = start_ts - BUFFER_MS
    result = await db.execute_query(base_query, (symbol, exchange, buffered_start, end_ts), fetch="all")
    output = []
    if not result:
        return output
    for row in result:
        is_mt = row[1] == "MT"
        output.append({
            "ts": int(row[0]),
            "symbol": row[1],
            "exchange": row[2],
            "o": float(row[3]) if row[3] is not None else None,
            "h": float(row[4]) if row[4] is not None else None,
            "l": float(row[5]) if row[5] is not None else None,
            "c": float(row[6]) if row[6] is not None else None,
            "v": float(row[7]) if row[7] is not None else None,
            "oi": None if is_mt else (float(row[8]) if row[8] is not None else None),
            "pfr": None if is_mt else (float(row[9]) if row[9] is not None else None),
            "lsr": None if is_mt else (float(row[10]) if row[10] is not None else None),
            "lql": None if is_mt else (float(row[11]) if row[11] is not None else None),
            "lqs": None if is_mt else (float(row[12]) if row[12] is not None else None),
            "rsi": float(row[13]) if row[13] is not None else None,
            "tbv": None if is_mt else (float(row[14]) if row[14] is not None else None),
            "tsv": None if is_mt else (float(row[15]) if row[15] is not None else None),
        })
    return output

# ===================================================================
# Calculate percentage change metrics for an array of raw data
# ===================================================================
def calculate_metrics_for_exchange(data):
    if not data:
        return []

    # Sort data by timestamp ascending
    data.sort(key=lambda x: x["ts"])
    metrics = []
    for i, current in enumerate(data):
        metric = {**current}
        # Initialize all change fields to None
        for period in [1,5,10]:
            suffix = f"{period}m"
            for f in ["c", "v", "oi", "pfr", "lsr", "rsi", "tbv", "tsv", "lql", "lqs"]:
                metric[f"{f}_chg_{suffix}"] = None

        def calc_change(idx, suffix):
            if idx < 0 or idx >= len(data):
                return
            prev = data[idx]
            for f in ["c", "v", "oi", "pfr", "lsr", "rsi", "tbv", "tsv", "lql", "lqs"]:
                metric[f"{f}_chg_{suffix}"] = calculate_percent_change(current.get(f), prev.get(f))

        calc_change(i-1, "1m")
        calc_change(i-5, "5m")
        calc_change(i-10, "10m")
        metrics.append(metric)
    return metrics

# ===================================================================
# Insert batch of metrics using COPY & merge stage
# ===================================================================
async def insert_metrics_batch(db, metrics):
    if not metrics:
        return {"inserted": 0, "errors": 0}

    try:
        await db.copy_to_metrics_stage(metrics)
        await db.merge_metrics_stage()
        await db.clear_metrics_stage()
        return {"inserted": len(metrics), "errors": 0}
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "Insert Error", f"{e}")
        return {"inserted": 0, "errors": 1}

# ===================================================================
# Backfill metrics per symbol per exchange
# ===================================================================
async def backfill_symbol(db, symbol, retention_start, end_ts):
    total_inserted = 0
    total_errors = 0

    for exchange in ["bin", "byb", "okx"]:
        try:
            raw_data = await fetch_raw_data(db, symbol, exchange, retention_start, end_ts)
            if not raw_data:
                continue
            metrics = calculate_metrics_for_exchange(raw_data)
            if not metrics:
                continue
            result = await insert_metrics_batch(db, metrics)
            total_inserted += result["inserted"]
            total_errors += result["errors"]
        except Exception as e:
            await log_error(db, SCRIPT_NAME, "Backfill Error", f"{symbol} exchange {exchange} error: {e}")
            total_errors += 1

    return total_inserted, total_errors

# ===================================================================
# Backfill all symbols concurrency
# ===================================================================
async def backfill_all(db):
    now_ms = int(time.time() * 1000)
    retention_days = DB_RETENTION_DAYS or DEFAULT_RETENTION_DAYS
    retention_start = now_ms - retention_days * 24 * 3600 * 1000

    symbols = await fetch_distinct_symbols(db, retention_start)
    if "MT" not in symbols:
        symbols.append("MT")

    total_inserted = 0
    total_errors = 0
    processed = 0

    semaphore = asyncio.Semaphore(PARALLEL_SYMBOLS)

    async def worker(sym):
        nonlocal total_inserted, total_errors, processed
        async with semaphore:
            inserted, errors = await backfill_symbol(db, sym, retention_start, now_ms)
            total_inserted += inserted
            total_errors += errors
            processed += 1
            await log_status(db, SCRIPT_NAME, "Running",
                             f"Processed {processed}/{len(symbols)} symbols, inserted {inserted} rows, errors {errors}")

    tasks = [worker(sym) for sym in symbols]
    await asyncio.gather(*tasks)

    await log_status(db, SCRIPT_NAME, "Completed",
                     f"Backfill complete. Inserted {total_inserted} rows with {total_errors} errors.")
    print(f"Backfill complete: Inserted {total_inserted} rows with {total_errors} errors.")

# ===================================================================
# Graceful shutdown handler (don't close pool; shared)
# ===================================================================
def graceful_shutdown(db, signal_name):
    print(f"\n⚠️ Received {signal_name}, shutting down gracefully...")

# ===================================================================
# Main function
# ===================================================================
async def main():
    db = DBManager()
    if not db.conn:
        print("DB Connection failed; exiting.")
        return
    try:
        await backfill_all(db)
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "Fatal Error", str(e))
        raise

if __name__ == "__main__":
    import signal
    import sys

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    db = DBManager()

    def on_signal(signum, frame):
        graceful_shutdown(db, signal.strsignal(signum))
        loop.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    loop.run_until_complete(main())