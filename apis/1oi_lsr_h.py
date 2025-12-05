# apis/1oi_lsr_h.py. rev:1Dec 2025 ver:1; initial creation from js files, based on 1ohlcv_pfr_h2 template.
import asyncio
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta

from colorama import Fore
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

# ============================================================================
#  1. SCRIPT SETUP & CONFIGURATION
# ============================================================================
load_dotenv()

try:
    from back.api_utils import log_status, log_error, format_symbol
    from back.perp_input import DB_RETENTION_DAYS, BASE_SYMBOLS
    from back.proxy import get_next_connection, CONFIGURED_PROXIES
    from back.dbsetup import DBManager
except ImportError as e:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%H:%M:%S')
    logging.error(f"CRITICAL: Failed to import a required module: {e}")
    exit(1)

# --- User-Configurable Settings ---
SCRIPT_NAME = "1oi_lsr_h.py"
SCRIPT_DEF = "OI & LSR Backfill"
HEARTBEAT_INTERVAL_SECONDS = 15
BATCH_INSERT_SIZE = 20000
TOTAL_CONCURRENCY = 15
API_TIMEOUT = 45 # These APIs can be slower
INITIAL_BACKOFF_DELAY = 1.5
MAX_RETRIES = 5

# --- API Configuration ---
BINANCE_API_CONFIG = {
    "base_url": "https://fapi.binance.com",
    "oi": {
        "endpoint": "/futures/data/openInterestHist",
        "param_name": "period",
        "interval": "5m",
        "limit": 500
    },
    "lsr": {
        "endpoint": "/futures/data/globalLongShortAccountRatio",
        "param_name": "period",
        "interval": "5m",
        "limit": 500
    },
}

# ============================================================================
#  2. SHARED STATE & HELPER FUNCTIONS
# ============================================================================
class ScriptState:
    def __init__(self):
        self.records_fetched = defaultdict(int)
        self.total_records_inserted = 0

def deduplicate_batch(batch):
    # ... (code is unchanged)
    seen = set()
    unique_batch = []
    for record in batch:
        key = (record.get('ts'), record.get('symbol'))
        if key not in seen and all(k is not None for k in key):
            seen.add(key)
            unique_batch.append(record)
    return unique_batch

def process_data(raw_data, base_symbol, data_type):
    processed = []
    if not raw_data:
        return processed
        
    try:
        for record in raw_data:
            ts = int(record['timestamp'])
            value = None
            if data_type == 'oi':
                value = float(record['sumOpenInterestValue'])
            elif data_type == 'lsr':
                value = float(record['longShortRatio'])
            
            if value is not None:
                # SPECIAL LOGIC: Expand one 5-min record into five 1-min records
                for i in range(5):
                    minute_ts = ts + (i * 60 * 1000)
                    if data_type == 'oi':
                        processed.append({"ts": minute_ts, "symbol": base_symbol, "oi": value})
                    elif data_type == 'lsr':
                        processed.append({"ts": minute_ts, "symbol": base_symbol, "lsr": value})
    except (ValueError, IndexError, KeyError):
        # Errors will be logged by the calling function if processing fails
        pass
    return processed

# ============================================================================
#  3. CORE API & DATABASE LOGIC
# ============================================================================
async def fetch_data(session, symbol, config, state, start_time_ms, proxy_map, db_manager, api_name="UNKNOWN"):
    all_data = []
    current_start_time = start_time_ms
    end_time_ms = int(datetime.now().timestamp() * 1000)
    endpoint = BINANCE_API_CONFIG["base_url"] + config["endpoint"]
    
    while current_start_time < end_time_ms:
        proxy_url, conn_id = get_next_connection()
        proxy_name = proxy_map.get(conn_id, "Unknown")
        delay = INITIAL_BACKOFF_DELAY
        
        interval_ms = 5 * 60 * 1000 
        window_ms = config["limit"] * interval_ms
        next_end_time = min(current_start_time + window_ms, end_time_ms)

        params = {
            "symbol": symbol,
            config["param_name"]: config["interval"],
            "startTime": current_start_time,
            "endTime": next_end_time,
            "limit": config["limit"]
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = await session.get(endpoint, params=params, proxies={"https": proxy_url}, timeout=API_TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        # This means we've reached the end of the available historical data
                        current_start_time = end_time_ms # End the loop
                        break 
                    
                    state.records_fetched[proxy_name] += len(data)
                    all_data.extend(data)
                    last_ts = int(data[-1]['timestamp'])
                    current_start_time = last_ts + interval_ms # Move to the next 5-min interval
                    
                    if len(data) < config["limit"]:
                        current_start_time = end_time_ms # End the loop
                    break
                else:
                    await log_error(db_manager, SCRIPT_NAME, "API Error", f"{proxy_name} | {symbol} | {api_name} | HTTP {response.status_code} | Retrying...")
                    await asyncio.sleep(delay)
                    delay *= 2
            except Exception as e:
                error_msg = e.__class__.__name__.replace("Error", "")
                await log_error(db_manager, SCRIPT_NAME, "Client Error", f"{proxy_name} | {symbol} | {api_name} | {error_msg} | Retrying...")
                if attempt >= MAX_RETRIES - 1:
                    current_start_time = end_time_ms # End the loop on persistent failure
                await asyncio.sleep(delay)
                delay *= 2
    return all_data

async def fetch_and_process_symbol(session, base_symbol, semaphore, db_manager, state, proxy_map):
    async with semaphore:
        binance_symbol = format_symbol(base_symbol, "binance")
        start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS)).timestamp() * 1000)

        results = await asyncio.gather(
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["oi"], state, start_time_ms, proxy_map, db_manager, "OI"),
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["lsr"], state, start_time_ms, proxy_map, db_manager, "LSR"),
            return_exceptions=True
        )
        raw_oi, raw_lsr = results if len(results) == 2 else ([], [])
        
        processed_oi = process_data(raw_oi, base_symbol, 'oi')
        if raw_oi and not processed_oi:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", f"Failed to process OI data for {base_symbol}")

        processed_lsr = process_data(raw_lsr, base_symbol, 'lsr')
        if raw_lsr and not processed_lsr:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", f"Failed to process LSR data for {base_symbol}")

        if processed_oi:
            for i in range(0, len(processed_oi), BATCH_INSERT_SIZE):
                batch = deduplicate_batch(processed_oi[i:i + BATCH_INSERT_SIZE])
                state.total_records_inserted += len(batch)
                await asyncio.to_thread(db_manager.insert_batch_data, batch)
        
        if processed_lsr:
            for i in range(0, len(processed_lsr), BATCH_INSERT_SIZE):
                batch = deduplicate_batch(processed_lsr[i:i + BATCH_INSERT_SIZE])
                state.total_records_inserted += len(batch)
                await asyncio.to_thread(db_manager.insert_batch_data, batch)

async def run_backfill(session, db_manager, state, proxy_map):
    semaphore = asyncio.Semaphore(TOTAL_CONCURRENCY)
    tasks = [fetch_and_process_symbol(session, sym, semaphore, db_manager, state, proxy_map) for sym in BASE_SYMBOLS]
    await asyncio.gather(*tasks)

# ============================================================================
#  4. MAIN EXECUTION BLOCK
# ============================================================================
async def main():
    db_manager = DBManager()
    if not db_manager.conn: return

    script_start_time = time.time()
    state = ScriptState()
    
    proxy_map = {url: f"Proxy{i+1}" for i, url in enumerate(CONFIGURED_PROXIES)}
    proxy_map["local"] = "Local"

    await log_status(db_manager, SCRIPT_NAME, "Started", f"🚀 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols", proxies=CONFIGURED_PROXIES, details={"days": DB_RETENTION_DAYS})
    
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            fetched_stats = "; ".join([f"{name}: {count:,}" for name, count in sorted(state.records_fetched.items())])
            print(Fore.CYAN + f"{SCRIPT_DEF} | Fetched: {fetched_stats} | Inserted: ~{state.total_records_inserted:,}")

    heartbeat_task = asyncio.create_task(heartbeat())
    
    try:
        async with AsyncSession() as session:
            await run_backfill(session, db_manager, state, proxy_map)
            
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Unhandled Exception", f"💥 Aborting Script: {e}", details=traceback.format_exc())
    
    finally:
        heartbeat_task.cancel()
        duration = time.time() - script_start_time
        
        completion_message = f"✅ {SCRIPT_DEF} Finished - Inserted {state.total_records_inserted:,} records in {duration:.2f}s."
        await log_status(db_manager, SCRIPT_NAME, "Completed", completion_message, {"duration": f"{duration:.2f}s"})
        
        db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
