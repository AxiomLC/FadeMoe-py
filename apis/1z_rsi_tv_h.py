# apis/1z_rsi_tv_h.py. rev:13Dec 2025 ver:8; Added OHLCV check & forward-fill logic
import asyncio
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

from colorama import Fore, Style
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
SCRIPT_NAME = "1z_rsi_tv_h.py"
SCRIPT_DEF = "RSI & TV Backfill"
HEARTBEAT_INTERVAL_SECONDS = 15
FINAL_LOOP_MINUTES = 2
BATCH_INSERT_SIZE = 5000
TOTAL_CONCURRENCY = 20
API_TIMEOUT = 5
RSI_PERIOD = 14
OHLCV_ERROR_THRESHOLD = 20.0  # Stop script if >5% OHLCV missing

# --- API Configuration ---
BINANCE_API_CONFIG = { "base_url": "https://fapi.binance.com", "tv": { "endpoint": "/futures/data/takerlongshortRatio", "interval": "5m", "limit": 500 } }

# ============================================================================
#  2. LOGGING, STATE, & HELPERS
# ============================================================================
LOG_CYAN = Fore.CYAN
LOG_YELLOW = Fore.YELLOW
LOG_RED = Fore.RED
LOG_BOLD_WHITE = Style.BRIGHT + Fore.WHITE

class ScriptState:
    def __init__(self):
        self.records_inserted = 0
        self.calculations_done = 0
        self.tv_fetched = 0
        self.missing_ohlcv_windows = defaultdict(set)
        self.no_tv_data_symbols = []

def floor_to_minute(ts_ms):
    return int(ts_ms / 60000) * 60000

# ============================================================================
#  3. OHLCV VALIDATION
# ============================================================================
async def validate_ohlcv_completeness(db_manager, symbols):
    """
    Check if OHLCV data is sufficient for RSI calculation.
    Returns (is_valid, missing_pct, total_expected, total_found).
    """
    try:
        # Calculate expected rows (1 per minute for DB_RETENTION_DAYS)
        expected_rows_per_symbol = DB_RETENTION_DAYS * 24 * 60
        total_expected = expected_rows_per_symbol * len(symbols)
        
        # Count actual OHLCV records
        query = "SELECT COUNT(*) FROM perp_data WHERE symbol = ANY(%s) AND c IS NOT NULL"
        result = await db_manager.execute_query(query, (symbols,), fetch="one")
        total_found = result[0] if result else 0
        
        missing_pct = ((total_expected - total_found) / total_expected * 100) if total_expected > 0 else 0
        is_valid = missing_pct <= OHLCV_ERROR_THRESHOLD
        
        return is_valid, missing_pct, total_expected, total_found
    except Exception as e:
        print(f"{LOG_RED}❌ OHLCV validation failed: {e}{Style.RESET_ALL}")
        return False, 100.0, 0, 0

# ============================================================================
#  4. RSI CALCULATION LOGIC (COPY-STAGE INSERT)
# ============================================================================
async def calculate_and_insert_rsi(db_manager, symbol, state):
    try:
        # Query close prices for this symbol
        query = "SELECT ts, c::numeric FROM perp_data WHERE symbol = %s AND c IS NOT NULL ORDER BY ts ASC"
        rows = await db_manager.execute_query(query, (symbol,), fetch="all")
        
        if not rows or len(rows) < RSI_PERIOD:
            return

        # Fast numpy-based RSI calculation
        closes = np.array([float(row[1]) for row in rows])
        
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.convolve(gains, np.ones(RSI_PERIOD)/RSI_PERIOD, mode='valid')
        avg_loss = np.convolve(losses, np.ones(RSI_PERIOD)/RSI_PERIOD, mode='valid')
        
        rs = avg_gain / np.where(avg_loss == 0, 1e-10, avg_loss)
        rsi_values = 100 - (100 / (1 + rs))
        
        # Align with timestamps
        rsi_records = []
        for i in range(len(rsi_values)):
            idx = i + RSI_PERIOD
            if idx < len(rows):
                rsi_records.append({
                    "ts": int(rows[idx][0]), 
                    "symbol": symbol, 
                    "rsi": float(rsi_values[i])
                })

        # Forward-fill RSI to NOW with last known value
        if rsi_records:
            last_ts = rsi_records[-1]['ts']
            now_ts = floor_to_minute(int(datetime.now(timezone.utc).timestamp() * 1000))
            last_rsi_value = rsi_records[-1]['rsi']
            
            current_ts = last_ts + 60000
            while current_ts <= now_ts:
                rsi_records.append({"ts": current_ts, "symbol": symbol, "rsi": last_rsi_value})
                current_ts += 60000
        
        # Use COPY-stage insert
        if rsi_records:
            inserted = await db_manager.merge_data_to_stage(rsi_records)
            state.calculations_done += len(rsi_records)
            state.records_inserted += inserted
            
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "RSI Error", f"Failed to calculate RSI for {symbol}: {e}")

# ============================================================================
#  5. TAKER VOLUME LOGIC (COPY-STAGE INSERT)
# ============================================================================
async def fetch_and_process_tv_for_symbol(session, symbol, db_manager, state, proxy_map, is_final_loop=False):
    binance_symbol = format_symbol(symbol, 'binance')
    start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS if not is_final_loop else 0, minutes=FINAL_LOOP_MINUTES if is_final_loop else 0)).timestamp() * 1000)
    
    # 1. Fetch
    binance_sym, raw_tv_data = await fetch_tv_data(session, binance_symbol, state, start_time_ms, proxy_map, db_manager)
    
    # 2. Process and Insert
    await process_and_insert_tv(db_manager, symbol, raw_tv_data, state)

async def fetch_tv_data(session, symbol, state, start_time_ms, proxy_map, db_manager):
    all_data = []
    current_start_time = start_time_ms
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    config = BINANCE_API_CONFIG['tv']
    endpoint = BINANCE_API_CONFIG["base_url"] + config["endpoint"]

    while current_start_time < end_time_ms:
        proxy_url, _ = get_next_connection()
        delay = 1.5
        interval_ms = 5 * 60 * 1000 
        window_ms = config["limit"] * interval_ms
        next_end_time = min(current_start_time + window_ms, end_time_ms)
        params = {"symbol": symbol, "period": config["interval"], "startTime": current_start_time, "endTime": next_end_time, "limit": config["limit"]}

        for _ in range(5):
            try:
                response = await session.get(endpoint, params=params, proxies={"proxy": proxy_url}, timeout=API_TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        current_start_time = end_time_ms
                        break
                    state.tv_fetched += len(data)
                    all_data.extend(data)
                    last_ts = int(data[-1]['timestamp'])
                    current_start_time = last_ts + interval_ms
                    if len(data) < config["limit"]:
                        current_start_time = end_time_ms
                    break
                else: await asyncio.sleep(delay)
            except Exception: await asyncio.sleep(delay)
    
    return (symbol, all_data)

async def process_and_insert_tv(db_manager, symbol, raw_tv_data, state):
    if not raw_tv_data:
        state.no_tv_data_symbols.append(symbol)
        return
    
    # FETCH ALL OHLCV DATA ONCE
    min_ts = int(raw_tv_data[0]['timestamp'])
    max_ts = int(raw_tv_data[-1]['timestamp']) + (5 * 60 * 1000)
    query = "SELECT ts, v::numeric FROM perp_data WHERE symbol = %s AND ts >= %s AND ts < %s AND v IS NOT NULL ORDER BY ts ASC"
    all_ohlcv = await db_manager.execute_query(query, (symbol, min_ts, max_ts), fetch="all")
    
    # Create lookup dict for O(1) access
    ohlcv_map = {int(row[0]): float(row[1]) for row in all_ohlcv} if all_ohlcv else {}
    
    all_tv_records = []
    last_valid_tbv = None
    last_valid_tsv = None
    
    # Handle incomplete current 5m bar
    last_bar = raw_tv_data[-1]
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    last_bar_ts = int(last_bar['timestamp'])
    
    if now_ts - last_bar_ts < (5 * 60 * 1000):
        minutes_elapsed = min(5, max(1, (now_ts - last_bar_ts) // 60000))
        buy_vol_per_min = float(last_bar['buyVol']) / minutes_elapsed
        sell_vol_per_min = float(last_bar['sellVol']) / minutes_elapsed
        for i in range(minutes_elapsed):
            minute_ts = last_bar_ts + (i * 60000)
            all_tv_records.append({"ts": minute_ts, "symbol": symbol, "tbv": buy_vol_per_min, "tsv": sell_vol_per_min})
            last_valid_tbv = buy_vol_per_min
            last_valid_tsv = sell_vol_per_min
        raw_tv_data = raw_tv_data[:-1]

    # Process complete 5m bars using the lookup dict
    for tv_point in raw_tv_data:
        ts_start = int(tv_point['timestamp'])
        total_buy_vol = float(tv_point['buyVol'])
        total_sell_vol = float(tv_point['sellVol'])
        
        # Get OHLCV rows for this 5min window from the map
        window_volumes = []
        for i in range(5):
            ts = ts_start + (i * 60000)
            if ts in ohlcv_map:
                window_volumes.append((ts, ohlcv_map[ts]))
        
        if not window_volumes:
            state.missing_ohlcv_windows[ts_start].add(symbol)
            for i in range(5):
                all_tv_records.append({"ts": ts_start + (i * 60000), "symbol": symbol, "tbv": total_buy_vol / 5, "tsv": total_sell_vol / 5})
                last_valid_tbv = total_buy_vol / 5
                last_valid_tsv = total_sell_vol / 5
        else:
            total_volume = sum(v for _, v in window_volumes)
            if total_volume > 0:
                for ts, vol in window_volumes:
                    weight = vol / total_volume
                    tbv_val = total_buy_vol * weight
                    tsv_val = total_sell_vol * weight
                    all_tv_records.append({"ts": ts, "symbol": symbol, "tbv": tbv_val, "tsv": tsv_val})
                    last_valid_tbv = tbv_val
                    last_valid_tsv = tsv_val
            else:
                for ts, _ in window_volumes:
                    tbv_val = total_buy_vol / len(window_volumes)
                    tsv_val = total_sell_vol / len(window_volumes)
                    all_tv_records.append({"ts": ts, "symbol": symbol, "tbv": tbv_val, "tsv": tsv_val})
                    last_valid_tbv = tbv_val
                    last_valid_tsv = tsv_val

    # Forward-fill TV to NOW with last known values
    if all_tv_records and last_valid_tbv is not None and last_valid_tsv is not None:
        now_ts = floor_to_minute(int(datetime.now(timezone.utc).timestamp() * 1000))
        last_record = max(all_tv_records, key=lambda x: x['ts'])
        last_ts = last_record['ts']
        current_ts = last_ts + 60000
        while current_ts <= now_ts:
            all_tv_records.append({"ts": current_ts, "symbol": symbol, "tbv": last_valid_tbv, "tsv": last_valid_tsv})
            current_ts += 60000

    # Use COPY-stage insert
    if all_tv_records:
        inserted = await db_manager.merge_data_to_stage(all_tv_records)
        state.records_inserted += inserted

def print_missing_ohlcv_summary(state):
    if not state.missing_ohlcv_windows:
        return
    
    # Find the most recent gap timestamp
    most_recent_gap_ts = max(state.missing_ohlcv_windows.keys())
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    minutes_gap = (now_ts - most_recent_gap_ts) // 60000
    
    print(f"{LOG_YELLOW}⚠️  Gap Detect: {minutes_gap} minutes db gap to last ohlcv.{Style.RESET_ALL}\n")

def print_no_tv_data_summary(state):
    if not state.no_tv_data_symbols:
        return
    print(f"\n{LOG_YELLOW}⚠️  No TV Data Received from API:")
    print(f"{LOG_YELLOW}   └─ {len(state.no_tv_data_symbols)} symbols: {', '.join(sorted(state.no_tv_data_symbols))}{Style.RESET_ALL}\n")

# ============================================================================
#  6. MAIN ORCHESTRATION
# ============================================================================
async def main():
    db_manager = DBManager()
    if not db_manager.conn: return

    script_start_time = time.time()
    state = ScriptState()
    
    proxy_map = {url: f"Proxy{i+1}" for i, url in enumerate(CONFIGURED_PROXIES)}
    proxy_map["local"] = "Local"

    await log_status(db_manager, SCRIPT_NAME, "Started", f"🚀 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols", proxies=CONFIGURED_PROXIES, details={"days": DB_RETENTION_DAYS})
    
    # --- VALIDATE OHLCV COMPLETENESS ---
    print(f"{LOG_CYAN}Validating OHLCV data completeness...{Style.RESET_ALL}")
    is_valid, missing_pct, expected, found = await validate_ohlcv_completeness(db_manager, BASE_SYMBOLS)
    
    if not is_valid:
        error_msg = f"❌ OHLCV DATA INSUFFICIENT! Missing {missing_pct:.2f}% (Found: {found:,}/{expected:,}). Threshold: {OHLCV_ERROR_THRESHOLD}%"
        print(f"{LOG_RED}{error_msg}{Style.RESET_ALL}")
        await log_error(db_manager, SCRIPT_NAME, "OHLCV Validation Failed", error_msg)
        db_manager.close_connection()
        return
    
    print(f"{LOG_CYAN}✓ OHLCV OK: {found:,}/{expected:,} records ({100-missing_pct:.2f}% complete){Style.RESET_ALL}\n")
    
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            print(Fore.CYAN + f"{SCRIPT_DEF} | RSI Calcs: ~{state.calculations_done:,} | TV Fetched: ~{state.tv_fetched:,} | Inserted: ~{state.records_inserted:,}")

    heartbeat_task = asyncio.create_task(heartbeat())
    
    try:
        async with AsyncSession() as session:
            # --- PHASE 1: SLOW NETWORK I/O ---
            await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 1: Fetching all Taker Volume data...")
            start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS)).timestamp() * 1000)
            tv_fetch_tasks = [fetch_tv_data(session, format_symbol(sym, 'binance'), state, start_time_ms, proxy_map, db_manager) for sym in BASE_SYMBOLS]
            tv_results = await asyncio.gather(*tv_fetch_tasks, return_exceptions=True)
            
            raw_tv_map = {}
            for i, result in enumerate(tv_results):
                if isinstance(result, Exception):
                    await log_error(db_manager, SCRIPT_NAME, "Fetch Error", f"Failed to fetch TV data for symbol {BASE_SYMBOLS[i]}: {result}")
                    raw_tv_map[BASE_SYMBOLS[i]] = []
                else:
                    binance_sym, data = result
                    raw_tv_map[BASE_SYMBOLS[i]] = data

            # --- PHASE 2: PARALLEL CPU and DB I/O ---
            await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 2: Processing/inserting all TV data and calculating RSI...")
            
            # Get all unique symbols from database
            query = "SELECT DISTINCT symbol FROM perp_data WHERE c IS NOT NULL"
            symbol_rows = await db_manager.execute_query(query, fetch="all")
            all_db_symbols = [row[0] for row in symbol_rows] if symbol_rows else []
            
            rsi_tasks = [calculate_and_insert_rsi(db_manager, sym, state) for sym in all_db_symbols]
            tv_process_tasks = [process_and_insert_tv(db_manager, sym, raw_tv_map.get(sym, []), state) for sym in BASE_SYMBOLS]
            await asyncio.gather(*(rsi_tasks + tv_process_tasks))
            
            print_missing_ohlcv_summary(state)

            # --- FINAL LOOP ---
            await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 3: Starting Final Loop for TV and final RSI calcs...")
            
            state.no_tv_data_symbols.clear()
            
            final_tv_tasks = [fetch_and_process_tv_for_symbol(session, sym, db_manager, state, proxy_map, is_final_loop=True) for sym in BASE_SYMBOLS]
            await asyncio.gather(*final_tv_tasks)
            
            print_no_tv_data_summary(state)
            
            # Final RSI calc for all symbols in database
            final_rsi_tasks = [calculate_and_insert_rsi(db_manager, sym, state) for sym in all_db_symbols]
            await asyncio.gather(*final_rsi_tasks)
            await log_status(db_manager, SCRIPT_NAME, "Running", "-- Final Data Population Complete --")

    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Unhandled Exception", f"💥 Aborting Script: {e}", details=traceback.format_exc())
    
    finally:
        heartbeat_task.cancel()
        duration = time.time() - script_start_time
        
        completion_message = f"⏱️ {SCRIPT_DEF} Complete - Inserted/Updated {state.records_inserted:,} records in {duration:.2f}s."
        await log_status(db_manager, SCRIPT_NAME, "Completed", completion_message, details={"duration": f"{duration:.2f}s"})
        
        db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())