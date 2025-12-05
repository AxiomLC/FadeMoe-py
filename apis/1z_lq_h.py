# apis/1z_lq_h.py. rev:2Dec 2025 ver:1; initial creation from js file for Coinalyze liquidations.
import asyncio
import time
import traceback
import os
from datetime import datetime, timezone, timedelta
from colorama import Fore, Style
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

# ============================================================================
#  1. SCRIPT SETUP & CONFIGURATION
# ============================================================================
load_dotenv()

try:
    from back.api_utils import log_status, log_error
    from back.perp_input import DB_RETENTION_DAYS, BASE_SYMBOLS
    from back.proxy import get_next_connection
    from back.dbsetup import DBManager
except ImportError as e:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%H:%M:%S')
    logging.error(f"CRITICAL: Failed to import a required module: {e}")
    exit(1)

# --- User-Configurable Settings ---
SCRIPT_NAME = "1z_lq_h.py"
SCRIPT_DEF = "LQ Backfill (Coinalyze)"
HEARTBEAT_INTERVAL_SECONDS = 15
BATCH_INSERT_SIZE = 20000
TOTAL_CONCURRENCY = 10 # Coinalyze may have stricter rate limits
API_TIMEOUT = 45
USE_PROXIES = False # Recommended: False. Paid APIs are often less restrictive.

# --- API Configuration ---
COINALYZE_KEY = os.getenv("COINALYZE_KEY")
API_URL = "https://api.coinalyze.net/v1/liquidation-history"

# ============================================================================
#  2. LOGGING, STATE, & HELPERS
# ============================================================================
LOG_CYAN = Fore.CYAN
LOG_BOLD_WHITE = Style.BRIGHT + Fore.WHITE

class ScriptState:
    def __init__(self):
        self.records_inserted = 0
        self.records_fetched = 0

def format_symbol_for_coinalyze(base_symbol):
    """Formats a base symbol for the Coinalyze API (Binance)."""
    return f"{base_symbol}USDT_PERP.A"

def process_data(raw_data, base_symbol):
    """Processes raw Coinalyze data into the database schema."""
    processed = []
    if not raw_data:
        return processed
    try:
        for record in raw_data:
            processed.append({
                "ts": int(record['t']) * 1000, # Convert seconds to milliseconds
                "symbol": base_symbol,
                "lql": float(record['l']),
                "lqs": float(record['s'])
            })
    except (ValueError, IndexError, KeyError) as e:
        # This will be caught and logged by the calling function
        raise ValueError(f"Failed to process record {record}: {e}")
    return processed

# ============================================================================
#  3. CORE API & DATABASE LOGIC
# ============================================================================
async def fetch_lq_data(session, symbol, start_time_s, end_time_s, state):
    """Fetches liquidation data for a single symbol from Coinalyze."""
    params = {
        "symbols": symbol,
        "interval": "1min",
        "from": start_time_s,
        "to": end_time_s,
        "convert_to_usd": "true"
    }
    headers = {"api_key": COINALYZE_KEY}
    proxy_url = None
    if USE_PROXIES:
        proxy_url, _ = get_next_connection()

    try:
        response = await session.get(API_URL, params=params, headers=headers, proxies={"https": proxy_url}, timeout=API_TIMEOUT)
        if response.status_code == 200:
            json_response = response.json()
            if json_response and json_response[0].get('history'):
                history = json_response[0]['history']
                state.records_fetched += len(history)
                return symbol, history
        else:
            await log_error(None, SCRIPT_NAME, "API Error", f"Failed for {symbol}: HTTP {response.status_code}")
    except Exception as e:
        error_msg = e.__class__.__name__.replace("Error", "")
        await log_error(None, SCRIPT_NAME, "Client Error", f"Failed for {symbol}: {error_msg}")
    
    return symbol, [] # Return empty list on failure

async def process_and_insert_lq(db_manager, symbol, raw_data, state):
    """Processes and inserts a symbol's fetched data."""
    try:
        # Coinalyze returns the full symbol, need to map it back to base
        base_symbol = symbol.split("USDT")[0]
        processed_records = process_data(raw_data, base_symbol)

        if processed_records:
            for i in range(0, len(processed_records), BATCH_INSERT_SIZE):
                batch = processed_records[i:i+BATCH_INSERT_SIZE]
                await asyncio.to_thread(db_manager.insert_batch_data, batch)
                state.records_inserted += len(batch)
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Processing/Insert Error", f"Failed for {symbol}: {e}")

# ============================================================================
#  4. MAIN ORCHESTRATION
# ============================================================================
async def run_backfill(session, db_manager, state):
    await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 1: Fetching all liquidation data...")
    
    start_time_s = int((datetime.now(timezone.utc) - timedelta(days=DB_RETENTION_DAYS)).timestamp())
    end_time_s = int(datetime.now(timezone.utc).timestamp())
    
    fetch_tasks = [
        fetch_lq_data(session, format_symbol_for_coinalyze(sym), start_time_s, end_time_s, state)
        for sym in BASE_SYMBOLS
    ]
    fetch_results = await asyncio.gather(*fetch_tasks)

    await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 2: Processing and inserting all liquidation data...")
    
    process_tasks = [
        process_and_insert_lq(db_manager, symbol, raw_data, state)
        for symbol, raw_data in fetch_results if raw_data
    ]
    await asyncio.gather(*process_tasks)

async def main():
    if not COINALYZE_KEY:
        print(LOG_RED + "CRITICAL: COINALYZE_KEY not found in .env file. Aborting.")
        return

    db_manager = DBManager()
    if not db_manager.conn: return

    script_start_time = time.time()
    state = ScriptState()
    
    await log_status(db_manager, SCRIPT_NAME, "Started", f"🚀 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols", details={"days": DB_RETENTION_DAYS})
    
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            print(LOG_CYAN + f"{SCRIPT_DEF} | Fetched: ~{state.records_fetched:,} | Inserted: ~{state.records_inserted:,}")

    heartbeat_task = asyncio.create_task(heartbeat())
    
    try:
        async with AsyncSession(impersonate="chrome110") as session:
            await run_backfill(session, db_manager, state)
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Unhandled Exception", f"💥 Aborting Script: {e}", details=traceback.format_exc())
    
    finally:
        heartbeat_task.cancel()
        duration = time.time() - script_start_time
        
        completion_message = f"✅ {SCRIPT_DEF} Finished - Inserted {state.records_inserted:,} records in {duration:.2f}s."
        await log_status(db_manager, SCRIPT_NAME, "Completed", completion_message, details={"duration": f"{duration:.2f}s"})
        
        db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
