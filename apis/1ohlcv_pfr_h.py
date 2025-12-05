# apis/1ohlcv_pfr_h.py. rev:1Dec 2025 ver:2; refactored to use shared async logging functions.
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
SCRIPT_NAME = "1ohlcv_pfr_h.py" # SCRIPT_NAME is for DB logging
SCRIPT_DEF = "OHLCV & PFR Backfill"
HEARTBEAT_INTERVAL_SECONDS = 15
FINAL_LOOP_MINUTES = 5
BATCH_INSERT_SIZE = 20000
TOTAL_CONCURRENCY = 20
API_TIMEOUT = 7
INITIAL_BACKOFF_DELAY = 1
MAX_RETRIES = 4
MT_SYMBOLS = ["ETH", "BTC", "XRP", "SOL"]

# --- API Configuration ---
BINANCE_API_CONFIG = {
    "base_url": "https://fapi.binance.com",
    "ohlcv": {"endpoint": "/fapi/v1/klines", "limit": 1500},
    "pfr": {"endpoint": "/fapi/v1/premiumIndexKlines", "limit": 1000},
}

# ============================================================================
#  2. SHARED STATE & HELPER FUNCTIONS
# ============================================================================
class ScriptState:
    def __init__(self):
        self.records_fetched = defaultdict(int)
        self.total_records_inserted = 0

def deduplicate_batch(batch):
    seen = set()
    unique_batch = []
    for record in batch:
        key = (record.get('ts'), record.get('symbol'))
        if key not in seen and all(k is not None for k in key):
            seen.add(key)
            unique_batch.append(record)
    return unique_batch

def process_data(raw_data, base_symbol, data_type):
    try:
        if data_type == 'ohlcv':
            return [{"ts": int(d[0]), "symbol": base_symbol, "o": float(d[1]), "h": float(d[2]), "l": float(d[3]), "c": float(d[4]), "v": float(d[7])} for d in raw_data]
        if data_type == 'pfr':
            return [{"ts": int(d[0]), "symbol": base_symbol, "pfr": float(d[4])} for d in raw_data]
    except (ValueError, IndexError):
        # Errors will be logged by the calling function if processing fails
        pass
    return []

def create_mt_symbol_data(all_ohlcv_data):
    mt_data_map, mt_records = {}, []
    for record in all_ohlcv_data:
        if record.get("symbol") in MT_SYMBOLS:
            mt_data_map.setdefault(record["ts"], []).append(record)
    for ts, records in mt_data_map.items():
        if len(records) == len(MT_SYMBOLS):
            mt_records.append({"ts": ts, "symbol": "MT", "o": sum(r["o"] for r in records) / len(records), "h": sum(r["h"] for r in records) / len(records), "l": sum(r["l"] for r in records) / len(records), "c": sum(r["c"] for r in records) / len(records), "v": sum(r["v"] for r in records)})
    return mt_records

# ============================================================================
#  3. CORE API & DATABASE LOGIC
# ============================================================================
async def fetch_data(session, symbol, config, state, start_time_ms, proxy_map, db_manager, is_final_loop=False, api_name="UNKNOWN"):
    all_data = []
    current_start_time = start_time_ms
    endpoint = BINANCE_API_CONFIG["base_url"] + config["endpoint"]
    
    pagination_active = True
    while pagination_active:
        proxy_url, conn_id = get_next_connection()
        proxy_name = proxy_map.get(conn_id, "Unknown")
        delay = INITIAL_BACKOFF_DELAY
        params = {"symbol": symbol, "interval": "1m", "startTime": current_start_time, "limit": config["limit"]}

        for attempt in range(MAX_RETRIES):
            try:
                response = await session.get(endpoint, params=params, proxies={"https": proxy_url}, timeout=API_TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        pagination_active = False
                        break
                    
                    state.records_fetched[proxy_name] += len(data)
                    all_data.extend(data)
                    last_ts = int(data[-1][0])
                    current_start_time = last_ts + 60000
                    
                    if datetime.fromtimestamp(last_ts / 1000) >= datetime.now() - timedelta(minutes=1):
                        pagination_active = False
                    if is_final_loop: pagination_active = False
                    break 
                else:
                    await log_error(db_manager, SCRIPT_NAME, "API Error", f"{proxy_name} | {symbol} | {api_name} | HTTP {response.status_code} | Retrying...")
                    await asyncio.sleep(delay)
                    delay *= 2
            except Exception as e:
                error_msg = e.__class__.__name__.replace("Error", "")
                await log_error(db_manager, SCRIPT_NAME, "Client Error", f"{proxy_name} | {symbol} | {api_name} | {error_msg} | Retrying...")
                if attempt >= MAX_RETRIES - 1: pagination_active = False
                await asyncio.sleep(delay)
                delay *= 2
        if not pagination_active: break
    return all_data

async def fetch_and_process_symbol(session, base_symbol, semaphore, db_manager, state, proxy_map, is_final_loop):
    async with semaphore:
        binance_symbol = format_symbol(base_symbol, "binance")
        start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS if not is_final_loop else 0, minutes=FINAL_LOOP_MINUTES if is_final_loop else 0)).timestamp() * 1000)

        results = await asyncio.gather(
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["ohlcv"], state, start_time_ms, proxy_map, db_manager, is_final_loop, "OHLCV"),
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["pfr"], state, start_time_ms, proxy_map, db_manager, is_final_loop, "PFR"),
            return_exceptions=True
        )
        raw_ohlcv, raw_pfr = results if len(results) == 2 else ([], [])
        
        processed_ohlcv = process_data(raw_ohlcv, base_symbol, 'ohlcv')
        if raw_ohlcv and not processed_ohlcv:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", f"Failed to process OHLCV data for {base_symbol}")

        processed_pfr = process_data(raw_pfr, base_symbol, 'pfr')
        if raw_pfr and not processed_pfr:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", f"Failed to process PFR data for {base_symbol}")
        
        if processed_ohlcv:
            for i in range(0, len(processed_ohlcv), BATCH_INSERT_SIZE):
                batch = deduplicate_batch(processed_ohlcv[i:i + BATCH_INSERT_SIZE])
                state.total_records_inserted += len(batch)
                await asyncio.to_thread(db_manager.insert_batch_data, batch)
        
        if processed_pfr:
            for i in range(0, len(processed_pfr), BATCH_INSERT_SIZE):
                batch = deduplicate_batch(processed_pfr[i:i + BATCH_INSERT_SIZE])
                state.total_records_inserted += len(batch)
                await asyncio.to_thread(db_manager.insert_batch_data, batch)
        
        return processed_ohlcv

async def run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=False):
    semaphore = asyncio.Semaphore(TOTAL_CONCURRENCY)
    tasks = [fetch_and_process_symbol(session, sym, semaphore, db_manager, state, proxy_map, is_final_loop) for sym in BASE_SYMBOLS]
    
    all_ohlcv_for_mt = []
    for future in asyncio.as_completed(tasks):
        ohlcv_list = await future
        if ohlcv_list: all_ohlcv_for_mt.extend(ohlcv_list)

    mt_symbol_data = create_mt_symbol_data(all_ohlcv_for_mt)
    if mt_symbol_data:
        for i in range(0, len(mt_symbol_data), BATCH_INSERT_SIZE):
            batch = deduplicate_batch(mt_symbol_data[i:i + BATCH_INSERT_SIZE])
            state.total_records_inserted += len(batch)
            await asyncio.to_thread(db_manager.insert_batch_data, batch)

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

    await log_status(db_manager, SCRIPT_NAME, "Started", f"🚀 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols", proxies=CONFIGURED_PROXIES)
    
    # --- Script-Specific Heartbeat (Console Log Only) ---
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            fetched_stats = "; ".join([f"{name}: {count:,}" for name, count in sorted(state.records_fetched.items())])
            # This is a console-only log and does not write to the DB
            print(Fore.CYAN + f"{SCRIPT_DEF} | Fetched: {fetched_stats} | Inserted: ~{state.total_records_inserted:,}")

    heartbeat_task = asyncio.create_task(heartbeat())
    
    try:
        async with AsyncSession() as session:
            await run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=False)
            await log_status(db_manager, SCRIPT_NAME, "Running", "Historical Backfill Complete - Starting Final Loop (to Now)")
            
            await run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=True)
            await log_status(db_manager, SCRIPT_NAME, "Running", "-- Final Loop Complete, MT token complete --")
            
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
