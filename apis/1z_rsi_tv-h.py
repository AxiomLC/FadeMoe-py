# apis/1z_rsi_tv-h.py. rev:2Dec 2025 ver:3; added MT token RSI calc and parallelized TV processing.
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
SCRIPT_DEF = "RSI & TV Backfill"
HEARTBEAT_INTERVAL_SECONDS = 15
BATCH_INSERT_SIZE = 20000
TOTAL_CONCURRENCY = 12 # Balanced for mixed network/DB load
API_TIMEOUT = 45
INITIAL_BACKOFF_DELAY = 1.5
MAX_RETRIES = 5
RSI_PERIOD = 14
MT_SYMBOLS = ["ETH", "BTC", "XRP", "SOL"]

# --- API Configuration ---
BINANCE_API_CONFIG = { "base_url": "https://fapi.binance.com", "tv": { "endpoint": "/futures/data/takerlongshortRatio", "interval": "5m", "limit": 500 } }

# ============================================================================
#  2. LOGGING, STATE, & HELPERS
# ============================================================================
LOG_CYAN = Fore.CYAN
LOG_BOLD_WHITE = Style.BRIGHT + Fore.WHITE
LOG_RED = Fore.RED

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
    try:
        if symbol == "MT":
            # Special case for MT Token: query and average component symbols
            query = """
                SELECT ts, AVG(o) as o, AVG(h) as h, AVG(l) as l, AVG(c) as c, SUM(v) as v
                FROM perp_data WHERE symbol = ANY(%s) AND c IS NOT NULL
                GROUP BY ts ORDER BY ts ASC
            """
            rows = await asyncio.to_thread(db_manager.execute_query, query, (MT_SYMBOLS,), fetch="all")
            # --- BUG FIX: Tell pandas to expect all 6 columns ---
            df = pd.DataFrame(rows, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        else:
            query = "SELECT ts, c::numeric FROM perp_data WHERE symbol = %s AND c IS NOT NULL ORDER BY ts ASC"
            rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol,), fetch="all")
            df = pd.DataFrame(rows, columns=['ts', 'c'])
        
        if not rows or len(rows) < RSI_PERIOD: return
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
#  4. TAKER VOLUME LOGIC (RE-ARCHITECTED FOR SPEED)
# ============================================================================
async def fetch_tv_data(session, symbol, config, state, start_time_ms, proxy_map, db_manager):
    # ... (This function is unchanged)
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

        params = {"symbol": symbol, "period": config["interval"], "startTime": current_start_time, "endTime": next_end_time, "limit": config["limit"]}

        for attempt in range(MAX_RETRIES):
            try:
                response = await session.get(endpoint, params=params, proxies={"https": proxy_url}, timeout=API_TIMEOUT)
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
                else:
                    await log_error(db_manager, SCRIPT_NAME, "API Error", f"{proxy_name} | {symbol} | TV | HTTP {response.status_code}")
                    await asyncio.sleep(delay)
                    delay *= 2
            except Exception as e:
                error_msg = e.__class__.__name__.replace("Error", "")
                await log_error(db_manager, SCRIPT_NAME, "Client Error", f"{proxy_name} | {symbol} | TV | {error_msg}")
                if attempt >= MAX_RETRIES - 1:
                    current_start_time = end_time_ms
                await asyncio.sleep(delay)
                delay *= 2
    return all_data

async def process_and_insert_tv(db_manager, raw_tv_data, symbol, state):
    # ... (This function is unchanged)
    all_tv_records = []
    for tv_point in raw_tv_data:
        try:
            ts_start = int(tv_point['timestamp'])
            ts_end = ts_start + (5 * 60 * 1000)
            total_buy_vol = float(tv_point['buyVol'])
            total_sell_vol = float(tv_point['sellVol'])

            query = "SELECT ts, c::numeric, v::numeric FROM perp_data WHERE symbol = %s AND ts >= %s AND ts < %s AND v IS NOT NULL AND c IS NOT NULL ORDER BY ts ASC"
            ohlcv_rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol, ts_start, ts_end), fetch="all")

            if not ohlcv_rows or len(ohlcv_rows) < 1:
                for i in range(5):
                    all_tv_records.append({"ts": ts_start + (i * 60000), "symbol": symbol, "tbv": total_buy_vol / 5, "tsv": total_sell_vol / 5})
                continue
            
            total_volume_in_window = sum(float(row[2]) for row in ohlcv_rows)
            if total_volume_in_window == 0:
                # Fallback to even split if total volume is zero
                for row in ohlcv_rows:
                     all_tv_records.append({"ts": int(row[0]), "symbol": symbol, "tbv": total_buy_vol / len(ohlcv_rows), "tsv": total_sell_vol / len(ohlcv_rows)})
                continue

            for row in ohlcv_rows:
                minute_ts, _, minute_vol_decimal = row
                weight = float(minute_vol_decimal) / total_volume_in_window
                all_tv_records.append({"ts": int(minute_ts), "symbol": symbol, "tbv": total_buy_vol * weight, "tsv": total_sell_vol * weight})

        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "TV Processing Error", f"Failed for {symbol} at {tv_point.get('timestamp')}: {e}")

    if all_tv_records:
        for i in range(0, len(all_tv_records), BATCH_INSERT_SIZE):
            batch = all_tv_records[i:i+BATCH_INSERT_SIZE]
            await asyncio.to_thread(db_manager.insert_batch_data, batch)
            state.records_inserted += len(batch)

# ============================================================================
#  5. MAIN ORCHESTRATION (RE-ARCHITECTED FOR SPEED)
# ============================================================================
async def run_backfill(session, db_manager, state, proxy_map):
    semaphore = asyncio.Semaphore(TOTAL_CONCURRENCY)
    
    # --- Part 1: RSI Calculation (Fast, DB-bound) ---
    # Create tasks for all symbols including the special 'MT' case
    rsi_symbols = BASE_SYMBOLS + ["MT"]
    rsi_tasks = [asyncio.create_task(calculate_and_insert_rsi(db_manager, sym, state)) for sym in rsi_symbols]

    # --- Part 2: Taker Volume Fetching (Slow, Network-bound) ---
    tv_fetch_tasks = []
    start_time_ms = int((datetime.now() - timedelta(days=DB_RETENTION_DAYS)).timestamp() * 1000)
    for sym in BASE_SYMBOLS:
        binance_symbol = format_symbol(sym, 'binance')
        task = asyncio.create_task(fetch_tv_data(session, binance_symbol, BINANCE_API_CONFIG["tv"], state, start_time_ms, proxy_map, db_manager))
        tv_fetch_tasks.append(task)
    
    # Run RSI calcs and TV fetching in parallel
    # We get the raw TV data back, mapping it to its symbol
    raw_tv_results = await asyncio.gather(*tv_fetch_tasks)
    raw_tv_map = {sym: data for sym, data in zip(BASE_SYMBOLS, raw_tv_results)}

    # --- Part 3: Taker Volume Processing (CPU/DB-bound) ---
    # Now that all slow fetching is done, process the results in parallel
    tv_process_tasks = [
        asyncio.create_task(process_and_insert_tv(db_manager, raw_tv_map[sym], sym, state))
        for sym in BASE_SYMBOLS if raw_tv_map.get(sym)
    ]
    
    # Await the completion of the TV processing and the already-running RSI tasks
    await asyncio.gather(*rsi_tasks, *tv_process_tasks)

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
            await run_backfill(session, db_manager, state, proxy_map)
            
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
