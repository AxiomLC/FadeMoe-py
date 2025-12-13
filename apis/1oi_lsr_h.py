# apis/1oi_lsr_h.py. rev:13Dec 2025 ver:3; Enhanced error handling with Binance pass-through
import asyncio
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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
FINAL_LOOP_MINUTES = 2
BATCH_INSERT_SIZE = 10000
TOTAL_CONCURRENCY = 9
API_TIMEOUT = 10
INITIAL_BACKOFF_DELAY = 1
MAX_RETRIES = 4

# --- Error Handling Configuration ---
RATE_LIMIT_FLOOD_THRESHOLD = 0.5  # 50% of symbols getting 429s = flood (adjustable)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
NON_RETRYABLE_STATUS_CODES = {418, 403, 400, 401, 451}  # These STOP the entire script

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
        self.rate_limit_symbols = set()  # Track which symbols hit 429
        self.script_should_stop = False  # Flag for catastrophic errors
        self.blacklisted_proxies = set()  # ADD THIS

def floor_to_minute(ts_ms):
    """Floor timestamp to nearest minute."""
    return int(ts_ms / 60000) * 60000

def extract_binance_error(response):
    """Extract Binance error message from response. Returns: (code, truncated_msg)"""
    try:
        error_data = response.json()
        code = error_data.get('code', response.status_code)
        msg = error_data.get('msg', '')
        # Truncate to first 5 words
        truncated = ' '.join(msg.split()[:5]) if msg else 'No description'
        return code, truncated
    except:
        return response.status_code, 'Unknown error'

def process_data(raw_data, base_symbol, data_type):
    """
    Process API response into 1-minute records.
    Expands 5-minute data into five 1-minute records with same value.
    Forward-fills to NOW with last known value.
    """
    processed = []
    if not raw_data:
        return processed
    
    last_known_value = None
    
    try:
        for record in raw_data:
            ts = int(record['timestamp'])
            value = None
            
            if data_type == 'oi':
                value = float(record['sumOpenInterestValue'])
            elif data_type == 'lsr':
                value = float(record['longShortRatio'])
            
            if value is not None:
                last_known_value = value
                # Expand one 5-min record into five 1-min records
                for i in range(5):
                    minute_ts = ts + (i * 60 * 1000)
                    if data_type == 'oi':
                        processed.append({"ts": minute_ts, "symbol": base_symbol, "oi": value})
                    elif data_type == 'lsr':
                        processed.append({"ts": minute_ts, "symbol": base_symbol, "lsr": value})
        
        # Forward-fill to NOW with last known value
        if processed and last_known_value is not None:
            last_ts = processed[-1]['ts']
            now_ts = floor_to_minute(int(datetime.now(timezone.utc).timestamp() * 1000))
            
            current_ts = last_ts + 60000
            while current_ts <= now_ts:
                if data_type == 'oi':
                    processed.append({"ts": current_ts, "symbol": base_symbol, "oi": last_known_value})
                elif data_type == 'lsr':
                    processed.append({"ts": current_ts, "symbol": base_symbol, "lsr": last_known_value})
                current_ts += 60000
                
    except (ValueError, IndexError, KeyError):
        pass  # Caller will log if raw_data exists but processing fails
    
    return processed

# ============================================================================
#  3. CORE API & DATABASE LOGIC
# ============================================================================
async def fetch_data(session, symbol, config, state, start_time_ms, proxy_map, db_manager, is_final_loop=False, api_name="UNKNOWN"):
    """Fetch data from Binance API with intelligent error handling."""
    all_data = []
    current_start_time = start_time_ms
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    endpoint = BINANCE_API_CONFIG["base_url"] + config["endpoint"]
    
    pagination_active = True
    while pagination_active and not state.script_should_stop:
        proxy_url, conn_id = get_next_connection()
        # Skip blacklisted proxies
        max_attempts = len(CONFIGURED_PROXIES) + 1
        for _ in range(max_attempts):
            proxy_url, conn_id = get_next_connection()
            if conn_id not in state.blacklisted_proxies:
                break
        else:
            # All proxies blacklisted
            await log_error(db_manager, SCRIPT_NAME, "Fatal", "All proxies blacklisted")
            state.script_should_stop = True
            return all_data
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
            if state.script_should_stop:
                return all_data
                
            try:
                response = await session.get(endpoint, params=params, proxies={"https": proxy_url}, timeout=API_TIMEOUT)
                
                # === SUCCESS ===
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        pagination_active = False
                        break 
                    
                    state.records_fetched[proxy_name] += len(data)
                    all_data.extend(data)
                    last_ts = int(data[-1]['timestamp'])
                    current_start_time = last_ts + interval_ms
                    
                    if datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc) >= datetime.now(timezone.utc) - timedelta(minutes=1):
                        pagination_active = False
                    if is_final_loop:
                        pagination_active = False
                    if len(data) < config["limit"]:
                        pagination_active = False
                    break
                
                # === NON-RETRYABLE ERRORS (STOP SCRIPT) ===
                elif response.status_code in NON_RETRYABLE_STATUS_CODES:
                    code, msg = extract_binance_error(response)
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | {code} {msg} | STOPPING SCRIPT"
                    await log_error(db_manager, SCRIPT_NAME, "Fatal API Error", error_msg)
                    print(Fore.RED + f"💀 FATAL: {error_msg}")
                    state.script_should_stop = True
                    return all_data
                
                # === RATE LIMIT (429) - RETRYABLE BUT TRACKED ===
                elif response.status_code == 429:
                    code, msg = extract_binance_error(response)
                    state.rate_limit_symbols.add(symbol)
                    
                    # Check for flood
                    flood_pct = len(state.rate_limit_symbols) / len(BASE_SYMBOLS)
                    if flood_pct >= RATE_LIMIT_FLOOD_THRESHOLD:
                        error_msg = f"429 FLOOD: {len(state.rate_limit_symbols)}/{len(BASE_SYMBOLS)} symbols rate-limited ({flood_pct:.0%}) | STOPPING SCRIPT"
                        await log_error(db_manager, SCRIPT_NAME, "Rate Limit Flood", error_msg)
                        print(Fore.RED + f"💀 {error_msg}")
                        state.script_should_stop = True
                        return all_data
                    
                    # Individual 429 - retry with backoff
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | 429 | Retrying..."
                    await log_error(db_manager, SCRIPT_NAME, "Rate Limit", error_msg)
                    if attempt >= MAX_RETRIES - 1:
                        pagination_active = False
                        break
                    await asyncio.sleep(delay)
                    delay *= 2
                
                # === OTHER RETRYABLE ERRORS (5xx) ===
                elif response.status_code in RETRYABLE_STATUS_CODES:
                    code, msg = extract_binance_error(response)
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | {code} {msg} | Retrying..."
                    await log_error(db_manager, SCRIPT_NAME, "API Error", error_msg)
                    if attempt >= MAX_RETRIES - 1:
                        pagination_active = False
                        break
                    await asyncio.sleep(delay)
                    delay *= 2
                
                # === UNKNOWN HTTP ERROR ===
                else:
                    code, msg = extract_binance_error(response)
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | {code} {msg}"
                    await log_error(db_manager, SCRIPT_NAME, "Unknown API Error", error_msg)
                    pagination_active = False
                    break
                    
            except asyncio.TimeoutError:
                error_msg = f"{proxy_name} | {symbol} | {api_name} | Timeout"
                await log_error(db_manager, SCRIPT_NAME, "Timeout", error_msg)
                if attempt >= MAX_RETRIES - 1:
                    pagination_active = False
                await asyncio.sleep(delay)
                delay *= 2
                
            except Exception as e:
                error_str = str(e).lower()
                # Proxy connection errors - mark as failed, continue with others
                if any(x in error_str for x in ['curl:', 'connection', 'proxy', 'ssl', 'certificate']):
                    state.blacklisted_proxies.add(conn_id)  # (internal tracking only)
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | {str(e)}"
                    await log_error(db_manager, SCRIPT_NAME, "Proxy Error", error_msg)
                    # Retry with different proxy
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(1)
                        continue
                    pagination_active = False
                    break
                # Code bugs
                else:
                    error_msg = f"{proxy_name} | {symbol} | {api_name} | {str(e)}"
                    await log_error(db_manager, SCRIPT_NAME, "Client Error", error_msg)
                    pagination_active = False
                    break
        
        if not pagination_active:
            break
            
    return all_data

async def fetch_and_process_symbol(session, base_symbol, semaphore, db_manager, state, proxy_map, is_final_loop=False):
    """Fetch and process data for a single symbol, then insert via COPY-stage."""
    async with semaphore:
        if state.script_should_stop:
            return
            
        binance_symbol = format_symbol(base_symbol, "binance")
        start_time_ms = int(
            (datetime.now(timezone.utc) - timedelta(
                days=DB_RETENTION_DAYS if not is_final_loop else 0,
                minutes=FINAL_LOOP_MINUTES if is_final_loop else 0
            )).timestamp() * 1000
        )

        results = await asyncio.gather(
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["oi"], state, start_time_ms, 
                      proxy_map, db_manager, is_final_loop, "OI"),
            fetch_data(session, binance_symbol, BINANCE_API_CONFIG["lsr"], state, start_time_ms, 
                      proxy_map, db_manager, is_final_loop, "LSR"),
            return_exceptions=True
        )
        raw_oi, raw_lsr = results if len(results) == 2 else ([], [])
        
        processed_oi = process_data(raw_oi, base_symbol, 'oi')
        if raw_oi and not processed_oi:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", 
                          f"Failed to process OI data for {base_symbol}")

        processed_lsr = process_data(raw_lsr, base_symbol, 'lsr')
        if raw_lsr and not processed_lsr:
            await log_error(db_manager, SCRIPT_NAME, "Processing Error", 
                          f"Failed to process LSR data for {base_symbol}")

        if processed_oi or processed_lsr:
            try:
                total_inserted = await db_manager.merge_data_to_stage(processed_oi, processed_lsr)
                state.total_records_inserted += total_inserted
            except Exception as e:
                await log_error(db_manager, SCRIPT_NAME, "Insert Error", 
                              f"{base_symbol} :: {e}", details=traceback.format_exc())

async def run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=False):
    """Run fetch cycle for all symbols."""
    semaphore = asyncio.Semaphore(TOTAL_CONCURRENCY)
    tasks = [fetch_and_process_symbol(session, sym, semaphore, db_manager, state, proxy_map, is_final_loop) 
             for sym in BASE_SYMBOLS]
    await asyncio.gather(*tasks)

# ============================================================================
#  4. MAIN EXECUTION BLOCK
# ============================================================================
async def main():
    db_manager = DBManager()
    if not db_manager.conn:
        return

    script_start_time = time.time()
    state = ScriptState()
    
    proxy_map = {url: f"Proxy{i+1}" for i, url in enumerate(CONFIGURED_PROXIES)}
    proxy_map["local"] = "Local"

    await log_status(db_manager, SCRIPT_NAME, "Started", 
                    f"🚀 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols", 
                    proxies=CONFIGURED_PROXIES, details={"days": DB_RETENTION_DAYS, "429_threshold": f"{RATE_LIMIT_FLOOD_THRESHOLD:.0%}"})
    
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            fetched_stats = "; ".join([f"{name}: {count:,}" for name, count in sorted(state.records_fetched.items())])
            rate_limit_info = f" | 429s: {len(state.rate_limit_symbols)}" if state.rate_limit_symbols else ""
            print(Fore.CYAN + f"🚥 {SCRIPT_DEF} | Fetched: {fetched_stats} | Inserted: ~{state.total_records_inserted:,}{rate_limit_info}")

    heartbeat_task = asyncio.create_task(heartbeat())
    
    try:
        async with AsyncSession() as session:
            await run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=False)
            
            if state.script_should_stop:
                await log_status(db_manager, SCRIPT_NAME, "Aborted", 
                               "❌ Script stopped due to fatal error")
            else:
                await log_status(db_manager, SCRIPT_NAME, "Running", 
                               "OI & LSR Backfill Complete - Starting Final Loop (to Now)")
                
                await run_fetch_cycle(session, db_manager, state, proxy_map, is_final_loop=True)
                
                if not state.script_should_stop:
                    await log_status(db_manager, SCRIPT_NAME, "Running", 
                                   "-- Final Loop Complete --")
            
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Unhandled Exception", 
                       f"💥 Aborting Script: {e}", details=traceback.format_exc())
    
    finally:
        heartbeat_task.cancel()
        duration = time.time() - script_start_time
        
        status = "Aborted" if state.script_should_stop else "Completed"
        completion_message = f"⏱️ {SCRIPT_DEF} {status} - Inserted {state.total_records_inserted:,} records in {duration:.2f}s."
        await log_status(db_manager, SCRIPT_NAME, status, completion_message, 
                        {"duration": f"{duration:.2f}s"})
        
        db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())