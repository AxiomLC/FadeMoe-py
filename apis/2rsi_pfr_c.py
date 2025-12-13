# apis/2rsi_pfr_c.py - rev: 5 Dec 2025 ver: 3
# Real-time PFR (Binance) + RSI polling script
import asyncio
import signal
import aiohttp
import pandas as pd
from datetime import datetime, timezone
from colorama import Fore, init as colorama_init
from back.api_utils import log_status, log_error
from back.dbsetup import DBManager
from back.perp_input import BASE_SYMBOLS

colorama_init()

# ============================================================================
# USER CONFIGURATION
# ============================================================================
SCRIPT_NAME = "2rsi_pfr_c.py"
SCRIPT_DEF = "Real-time PFR & RSI Poll"
STATUS_COLOR = Fore.LIGHTGREEN_EX
RESET = Fore.RESET
HEARTBEAT_INTERVAL = 60
RSI_PERIOD = 14
BINANCE_PFR_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
API_TIMEOUT = 10

# ============================================================================
# SHUTDOWN & STATE
# ============================================================================
def shutdown_handler(signum, frame):
    asyncio.create_task(shutdown())

async def shutdown():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks: task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

class ScriptState:
    def __init__(self):
        self.pfr_count = 0
        self.rsi_count = 0
    def reset(self):
        self.pfr_count = 0
        self.rsi_count = 0

def deduplicate_batch(batch):
    seen = set()
    unique = []
    for record in batch:
        key = (record['ts'], record['symbol'])
        if key not in seen:
            seen.add(key)
            unique.append(record)
    return unique

# ============================================================================
# GAP DETECTION
# ============================================================================
async def check_gaps(db_manager, lookback_minutes=2):
    try:
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        query_pfr = "SELECT MAX(ts) FROM perp_data WHERE pfr IS NOT NULL"
        result_pfr = await asyncio.to_thread(db_manager.execute_query, query_pfr, fetch="one")
        pfr_gap = (now - result_pfr[0]) // 60000 if result_pfr and result_pfr[0] else 0
        
        query_ohlcv = "SELECT MAX(ts) FROM perp_data WHERE c IS NOT NULL"
        result_ohlcv = await asyncio.to_thread(db_manager.execute_query, query_ohlcv, fetch="one")
        ohlcv_gap = (now - result_ohlcv[0]) // 60000 if result_ohlcv and result_ohlcv[0] else 0
        
        gaps = []
        if pfr_gap > lookback_minutes: gaps.append(f"PFR: {pfr_gap}min")
        if ohlcv_gap > lookback_minutes: gaps.append(f"OHLCV: {ohlcv_gap}min")
        
        if gaps:
            message = f"Gap detected - {', '.join(gaps)}"
            await log_error(db_manager, SCRIPT_NAME, "Gap Detect", message)
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Gap Check", str(e))

# ============================================================================
# PFR POLLING
# ============================================================================
async def fetch_binance_pfr(session, symbol):
    try:
        params = {"symbol": f"{symbol}USDT"}
        async with session.get(BINANCE_PFR_URL, params=params, timeout=API_TIMEOUT) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list): data = data[0]
                pfr = float(data['lastFundingRate'])
                return pfr if pfr == pfr else None
            return None
    except:
        return None

async def poll_pfr(session, db_manager, state):
    tasks = [fetch_binance_pfr(session, symbol) for symbol in BASE_SYMBOLS]
    results = await asyncio.gather(*tasks)
    
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    ts = now - (now % 60000)
    
    pfr_batch = [{"ts": ts, "symbol": symbol, "pfr": pfr} 
                 for symbol, pfr in zip(BASE_SYMBOLS, results) if pfr is not None]
    
    if pfr_batch:
        try:
            pfr_batch = deduplicate_batch(pfr_batch)
            await asyncio.to_thread(db_manager.insert_batch_data, pfr_batch)
            state.pfr_count = len(pfr_batch)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "PFR Insert", str(e))

# ============================================================================
# RSI CALCULATION
# ============================================================================
async def calculate_rsi_for_symbol(db_manager, symbol):
    try:
        query = """
        SELECT ts, c::numeric FROM perp_data 
        WHERE symbol = %s AND c IS NOT NULL 
        ORDER BY ts DESC
        LIMIT %s
        """
        rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol, RSI_PERIOD + 1), fetch="all")
        
        if not rows or len(rows) < RSI_PERIOD + 1:
            return None
        
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_ts = int(rows[0][0]) if isinstance(rows[0][0], int) else int(rows[0][0].timestamp() * 1000)
        
        if (now - last_ts) > 120000:
            return None
        
        df = pd.DataFrame(reversed(rows), columns=['ts', 'c'])
        df['c'] = pd.to_numeric(df['c'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        df = df.set_index('ts')
        
        delta = df['c'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df = df.dropna(subset=['rsi'])
        
        if df.empty:
            return None
        
        return float(df['rsi'].iloc[-1])
        
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "RSI Calc", f"{symbol}: {str(e)}")
        return None

async def poll_rsi(db_manager, state):
    query = "SELECT DISTINCT symbol FROM perp_data WHERE c IS NOT NULL"
    result = await asyncio.to_thread(db_manager.execute_query, query, fetch="all")
    symbols = [row[0] for row in result] if result else []
    
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    ts = now - (now % 60000)
    
    rsi_batch = []
    for symbol in symbols:
        rsi_value = await calculate_rsi_for_symbol(db_manager, symbol)
        if rsi_value is not None:
            rsi_batch.append({"ts": ts, "symbol": symbol, "rsi": rsi_value})
    
    if rsi_batch:
        try:
            rsi_batch = deduplicate_batch(rsi_batch)
            await asyncio.to_thread(db_manager.insert_batch_data, rsi_batch)
            state.rsi_count = len(rsi_batch)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "RSI Insert", str(e))

# ============================================================================
# MAIN EXECUTION
# ============================================================================
async def main():
    db_manager = DBManager()
    if not db_manager.conn: return
    
    state = ScriptState()
    
    await log_status(db_manager, SCRIPT_NAME, "Started", 
                    f"{STATUS_COLOR}🚦 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols.{RESET}")
    
    await check_gaps(db_manager)
    
    async with aiohttp.ClientSession() as session:
        try:
            while True:
                try:
                    await poll_pfr(session, db_manager, state)
                    await poll_rsi(db_manager, state)
                    
                    print(f"{STATUS_COLOR}🚥 {SCRIPT_DEF} | PFR: {state.pfr_count} | RSI: {state.rsi_count}{RESET}")
                    state.reset()
                    
                except Exception as e:
                    await log_error(db_manager, SCRIPT_NAME, "Poll Error", str(e))
                
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    asyncio.run(main())