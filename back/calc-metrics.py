# calc_metrics.py. rev:12Dec2025 ver:1; incremental rolling % change calc and bulk batch COPY insert to perp_metrics with concurrency and error handling.

import asyncio
import time
from datetime import datetime

from back.api_utils import log_status, log_error
from back.dbsetup2 import DBManager

SCRIPT_NAME = "calc_metrics.py"

DB_RETENTION_DAYS = 20
LOOKBACK_MINUTES = 15
BUFFER_MS = 10 * 60 * 1000  # 10 min buffer for edge cases
CALCULATION_INTERVAL_MS = 60000
HEARTBEAT_INTERVAL_MS = 15000

SYMBOL_BATCH_SIZE = 2
PARALLEL_SYMBOLS = 3

# ===========================================================================
# Async sleep helper
# ===========================================================================
async def sleep(ms):
    await asyncio.sleep(ms / 1000)

# ===========================================================================
# Percent change calculator
# ===========================================================================
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

# ===========================================================================
# Fetch symbols with recent data
# ===========================================================================
async def fetch_symbols(db, min_ts):
    q = "SELECT DISTINCT(symbol) FROM perp_metrics WHERE ts >= %s;"
    rows = await db.execute_query(q, (min_ts,), fetch="all")
    return [row[0] for row in rows] if rows else []

# ===========================================================================
# Fetch raw metrics for symbol and time window
# ===========================================================================
async def fetch_raw_metrics(db, symbol, start_ts, end_ts):
    q = """
        SELECT ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr, rsi, tbv, tsv, lql, lqs
        FROM perp_metrics
        WHERE symbol = %s AND ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """
    result = await db.execute_query(q, (symbol, start_ts, end_ts), fetch="all")
    raw = []
    if result:
        for row in result:
            raw.append({
                "ts": int(row[0]),
                "symbol": row[1],
                "exchange": row[2],
                "o": float(row[3]) if row[3] is not None else None,
                "h": float(row[4]) if row[4] is not None else None,
                "l": float(row[5]) if row[5] is not None else None,
                "c": float(row[6]) if row[6] is not None else None,
                "v": float(row[7]) if row[7] is not None else None,
                "oi": float(row[8]) if row[8] is not None else None,
                "pfr": float(row[9]) if row[9] is not None else None,
                "lsr": float(row[10]) if row[10] is not None else None,
                "rsi": float(row[11]) if row[11] is not None else None,
                "tbv": float(row[12]) if row[12] is not None else None,
                "tsv": float(row[13]) if row[13] is not None else None,
                "lql": float(row[14]) if row[14] is not None else None,
                "lqs": float(row[15]) if row[15] is not None else None,
            })
    return raw

# ===========================================================================
# Calculate rolling metrics on raw data with 1m, 5m, 10m change windows
# ===========================================================================
def calculate_rolling_metrics(data):
    if not data:
        return []

    data.sort(key=lambda x: x["ts"])
    metrics = []
    for i, current in enumerate(data):
        m = dict(current)
        for period in [1, 5, 10]:
            suffix = f"{period}m"
            for f in ["c", "v", "oi", "pfr", "lsr", "rsi", "tbv", "tsv", "lql", "lqs"]:
                m[f"{f}_chg_{suffix}"] = None

        def calc_change(idx, suffix):
            if idx < 0 or idx >= len(data):
                return
            prev = data[idx]
            for f in ["c", "v", "oi", "pfr", "lsr", "rsi", "tbv", "tsv", "lql", "lqs"]:
                m[f"{f}_chg_{suffix}"] = calculate_percent_change(current.get(f), prev.get(f))

        calc_change(i - 1, "1m")
        calc_change(i - 5, "5m")
        calc_change(i - 10, "10m")

        metrics.append(m)
    return metrics

# ===========================================================================
# Insert batch of calculated metrics using COPY staging
# ===========================================================================
async def insert_metrics_batch(db, metrics):
    if not metrics:
        return {"inserted": 0, "errors": 0}
    try:
        await db.copy_to_metrics_stage(metrics)
        await db.merge_metrics_stage()
        await db.clear_metrics_stage()
        return {"inserted": len(metrics), "errors": 0}
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "Insert Error", str(e))
        return {"inserted": 0, "errors": 1}

# ===========================================================================
# Process one symbol: fetch raw, calculate rolling metrics
# ===========================================================================
async def process_symbol(db, symbol, start_ts, end_ts):
    try:
        raw = await fetch_raw_metrics(db, symbol, start_ts, end_ts)
        if not raw:
            return {"success": False, "metrics": [], "symbol": symbol}
        metrics = calculate_rolling_metrics(raw)
        return {"success": True, "metrics": metrics, "symbol": symbol}
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "Calculation Error", f"{symbol}: {e}")
        return {"success": False, "metrics": [], "symbol": symbol}

# ===========================================================================
# Main batch processing with concurrency and batching
# ===========================================================================
async def calculate_all_metrics(db):
    now_ms = int(time.time() * 1000)
    retention_days = DB_RETENTION_DAYS or 20
    start_ts = now_ms - retention_days * 24 * 3600 * 1000 - BUFFER_MS

    symbols = await fetch_symbols(db, start_ts)
    total_inserted = 0
    total_errors = 0
    processed = 0
    semaphore = asyncio.Semaphore(PARALLEL_SYMBOLS)
    batch_metrics = []

    async def worker(sym):
        nonlocal total_inserted, total_errors, processed, batch_metrics
        async with semaphore:
            result = await process_symbol(db, sym, start_ts, now_ms)
            processed += 1
            if result["success"]:
                batch_metrics.extend(result["metrics"])
            else:
                total_errors += 1
            # ==========  Insert batch if size is enough or end of list ==================
            if len(batch_metrics) >= SYMBOL_BATCH_SIZE or processed == len(symbols):
                if batch_metrics:
                    batch_metrics.sort(key=lambda x: (x["ts"], x["symbol"], x.get("exchange", "")))
                    res = await insert_metrics_batch(db, batch_metrics)
                    total_inserted += res["inserted"]
                    total_errors += res["errors"]
                    batch_metrics.clear()

    tasks = [worker(sym) for sym in symbols]
    await asyncio.gather(*tasks)

    status = "completed" if total_errors == 0 else "partial"
    await log_status(db, SCRIPT_NAME, status,
                     f"Processed {processed} symbols, inserted {total_inserted} metrics with {total_errors} errors")
    print(f"Processed {processed} symbols, inserted {total_inserted} metrics with {total_errors} errors")

# ===========================================================================
# Graceful shutdown handler
# ===========================================================================
def graceful_shutdown(db, signal_name):
    print(f"Received {signal_name}, shutting down gracefully...")

# ===========================================================================
# Main entry point
# ===========================================================================
async def main():
    db = DBManager()
    if not db.conn:
        print("DB connection failure; exiting.")
        return
    try:
        await calculate_all_metrics(db)
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "Fatal Error", str(e))
        raise

if __name__ == "__main__":
    import signal
    import sys

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    db = DBManager()

    def shutdown_handler(signum, frame):
        graceful_shutdown(db, signal.strsignal(signum))
        loop.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    loop.run_until_complete(main())