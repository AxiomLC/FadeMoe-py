# apis/1z_rsi_tv-h.py. rev:2Dec 2025 ver:5; reverted to symbol-by-symbol architecture to fix deadlock, added Final Loop.
import asyncio
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pandas as pd

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
SCRIPT_NAME = "1z_rsi_tv-h.py"
SCRIPT_DEF = "RSI Calculation & Taker Volume Backfill"
HEARTBEAT_INTERVAL_SECONDS = 15
FINAL_LOOP_MINUTES = 5 # For TV Final Loop
BATCH_INSERT_SIZE = 20000
TOTAL_CONCURRENCY = 12
API_TIMEOUT = 45
RSI_PERIOD = 14
MT_SYMBOLS = ["ETH", "BTC", "XRP", "SOL"]

# --- API Configuration ---
BINANCE_API_CONFIG = { "base_url": "https://fapi.binance.com", "tv": { "endpoint": "/futures/data/takerlongshortRatio", "interval": "5m", "limit": 500 } }

# ============================================================================
#  2. LOGGING, STATE, & HELPERS
# ============================================================================
LOG_CYAN = Fore.CYAN
LOG_BOLD_WHITE = Style.BRIGHT + Fore.WHITE

class ScriptState:
    def __init__(self):
        self.records_inserted = 0
        self.calculations_done = 0
        self.tv_fetched = 0

def floor_to_minute(ts_ms):
    return int(ts_ms / 60000) * 60000

# ============================================================================
#  3. RSI CALCULATION LOGIC
# ============================================================================
async def calculate_and_insert_rsi(db_manager, symbol, state):
    # This function is now standalone and correct.
    try:
        if symbol == "MT":
            query = "SELECT ts, AVG(c)::numeric as c FROM perp_data WHERE symbol = ANY(%s) AND c IS NOT NULL GROUP BY ts ORDER BY ts ASC"
            rows = await asyncio.to_thread(db_manager.execute_query, query, (MT_SYMBOLS,), fetch="all")
        else:
            query = "SELECT ts, c::numeric FROM perp_data WHERE symbol = %s AND c IS NOT NULL ORDER BY ts ASC"
            rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol,), fetch="all")
        
        if not rows or len(rows) < RSI_PERIOD: return

        df = pd.DataFrame(rows, columns=['ts', 'c'])
        df['c'] = pd.to_numeric(df['c'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        df = df.set_index('ts')

        delta = df['c'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df = df.dropna(subset=['rsi'])
        
        rsi_records = [{"ts": int(ts.timestamp() * 1000), "symbol": symbol, "rsi": float(row['rsi'])} for ts, row in df.iterrows()]

        if rsi_records:
            last_ts = rsi_records[-1]['ts']
            now_ts = floor_to_minute(int(datetime.now(timezone.utc).timestamp() * 1000))
            last_rsi_value = rsi_records[-1]['rsi']
            
            current_ts = last_ts + 60000
            while current_ts <= now_ts:
                rsi_records.append({"ts": current_ts, "symbol": symbol, "rsi": last_rsi_value})
                current_ts += 60000
        
        if rsi_records:
            for i in range(0, len(rsi_records), BATCH_INSERT_SIZE):
                batch = rsi_records[i:i+BATCH_INSERT_SIZE]
                await asyncio.to_thread(db_manager.insert_batch_data, batch)
                state.calculations_done += len(batch)
                state.records_inserted += len(batch)
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "RSI Error", f"Failed to calculate RSI for {symbol}: {e}")

# ============================================================================
#  4. TAKER VOLUME LOGIC
# ============================================================================
async def fetch_and_process_tv_for_symbol(session, symbol, db_manager, state, proxy_map, is_final_loop=False):
    # This function now handles the full fetch->process->insert pipeline for one symbol.
    binance_symbol = format_symbol(symbol, 'binance')
    start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS if not is_final_loop else 0, minutes=FINAL_LOOP_MINUTES if is_final_loop else 0)).timestamp() * 1000)
    
    # 1. Fetch
    raw_tv_data = await fetch_tv_data(session, binance_symbol, state, start_time_ms, proxy_map, db_manager)
    
    # 2. Process and Insert
    await process_and_insert_tv(db_manager, symbol, raw_tv_data, state)

async def fetch_tv_data(session, symbol, state, start_time_ms, proxy_map, db_manager):
    # (This sub-function is mostly unchanged)
    all_data = []
    # ... (rest of the fetch logic is correct and remains)
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
    return all_data


async def process_and_insert_tv(db_manager, symbol, raw_tv_data, state):
    # (This sub-function is correct and remains)
    all_tv_records = []
    # ... (weighted distribution logic)
    if raw_tv_data:
        # ... up-to-now logic ...
        last_bar = raw_tv_data[-1]
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_bar_ts = int(last_bar['timestamp'])
        if now_ts - last_bar_ts < (5 * 60 * 1000):
            minutes_elapsed = max(1, (now_ts - last_bar_ts) // 60000 + 1)
            buy_vol_per_min = float(last_bar['buyVol']) / minutes_elapsed
            sell_vol_per_min = float(last_bar['sellVol']) / minutes_elapsed
            for i in range(minutes_elapsed):
                minute_ts = last_bar_ts + (i * 60000)
                all_tv_records.append({"ts": minute_ts, "symbol": symbol, "tbv": buy_vol_per_min, "tsv": sell_vol_per_min})
            raw_tv_data = raw_tv_data[:-1]

    for tv_point in raw_tv_data:
        ts_start = int(tv_point['timestamp'])
        total_buy_vol = float(tv_point['buyVol'])
        total_sell_vol = float(tv_point['sellVol'])
        query = "SELECT ts, v::numeric FROM perp_data WHERE symbol = %s AND ts >= %s AND ts < %s AND v IS NOT NULL ORDER BY ts ASC"
        ts_end = ts_start + (5 * 60 * 1000)
        ohlcv_rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol, ts_start, ts_end), fetch="all")
        if not ohlcv_rows:
            for i in range(5):
                all_tv_records.append({"ts": ts_start + (i * 60000), "symbol": symbol, "tbv": total_buy_vol / 5, "tsv": total_sell_vol / 5})
        else:
            total_volume_in_window = sum(float(row[1]) for row in ohlcv_rows)
            if total_volume_in_window > 0:
                for row in ohlcv_rows:
                    weight = float(row[1]) / total_volume_in_window
                    all_tv_records.append({"ts": int(row[0]), "symbol": symbol, "tbv": total_buy_vol * weight, "tsv": total_sell_vol * weight})
            elif len(ohlcv_rows) > 0:
                num_rows = len(ohlcv_rows)
                for i in range(num_rows):
                    all_tv_records.append({"ts": int(ohlcv_rows[i][0]), "symbol": symbol, "tbv": total_buy_vol / num_rows, "tsv": total_sell_vol / num_rows})

    if all_tv_records:
        for i in range(0, len(all_tv_records), BATCH_INSERT_SIZE):
            batch = all_tv_records[i:i+BATCH_INSERT_SIZE]
            await asyncio.to_thread(db_manager.insert_batch_data, batch)
            state.records_inserted += len(batch)

# ============================================================================
#  5. MAIN ORCHESTATION (FINAL PIPELINE ARCHITECTURE)
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
                elif result and len(result) == 2:
                    _, data = result # symbol is returned as binance_symbol, so we use our base symbol
                    raw_tv_map[BASE_SYMBOLS[i]] = data

            # --- PHASE 2: PARALLEL CPU and DB I/O ---
            await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 2: Calculating all RSI and processing/inserting all TV data...")
            rsi_tasks = [calculate_and_insert_rsi(db_manager, sym, state) for sym in BASE_SYMBOLS]
            tv_process_tasks = [process_and_insert_tv(db_manager, sym, raw_tv_map.get(sym, []), state) for sym in BASE_SYMBOLS]
            await asyncio.gather(*(rsi_tasks + tv_process_tasks))

            # --- FINAL LOOP ---
            await log_status(db_manager, SCRIPT_NAME, "Running", "Phase 3: Starting Final Loop for TV and final RSI calcs...")
            final_tv_tasks = [fetch_and_process_tv_for_symbol(session, sym, db_manager, state, proxy_map, is_final_loop=True) for sym in BASE_SYMBOLS]
            await asyncio.gather(*final_tv_tasks)
            
            # Final RSI calc for all symbols + MT Token
            final_rsi_tasks = [calculate_and_insert_rsi(db_manager, sym, state) for sym in BASE_SYMBOLS + ["MT"]]
            await asyncio.gather(*final_rsi_tasks)
            await log_status(db_manager, SCRIPT_NAME, "Running", "-- Final Data Population Complete --")

    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Unhandled Exception", f"💥 Aborting Script: {e}", details=traceback.format_exc())
    
    finally:
        heartbeat_task.cancel()
        duration = time.time() - script_start_time
        
        completion_message = f"✅ {SCRIPT_DEF} Finished - Inserted/Updated {state.records_inserted:,} records in {duration:.2f}s."
        await log_status(db_manager, SCRIPT_NAME, "Completed", completion_message, details={"duration": f"{duration:.2f}s"})
        
        db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
