# apis/2oi_lsr_c.py. rev:5Dec 2025 ver:3;
# Fixed: Cache future timestamps, only insert at proper time
import asyncio
import signal
import aiohttp
from datetime import datetime, timezone
from colorama import Fore, init as colorama_init
from back.api_utils import log_status, log_error
from back.dbsetup import DBManager
from back.perp_input import BASE_SYMBOLS

colorama_init()

def shutdown_handler(signum, frame):
    print(f"{Fore.YELLOW} Shutting down gracefully...{Fore.RESET}")
    asyncio.create_task(shutdown())

async def shutdown():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"{Fore.YELLOW}✔️ Shutdown complete.{Fore.RESET}")
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# Script configuration
SCRIPT_NAME = "2oi_lsr_c.py"
SCRIPT_DEF = "Real-time OI & LSR Poll"
STATUS_COLOR = Fore.LIGHTGREEN_EX
RESET = Fore.RESET
HEARTBEAT_INTERVAL = 60

# Binance API configuration
BINANCE_API_CONFIG = {
    "oi": {
        "url": "https://fapi.binance.com/futures/data/openInterestHist",
        "params": {"symbol": None, "period": "5m", "limit": 1}
    },
    "lsr": {
        "url": "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
        "params": {"symbol": None, "period": "5m", "limit": 1}
    }
}

# State for tracking
class ScriptState:
    def __init__(self):
        self.oi_symbols = set()  # Track unique symbols inserted
        self.lsr_symbols = set()
        self.cache = []  # Cache future records: [{ts, symbol, oi/lsr, value}]

    def reset_counts(self):
        self.oi_symbols.clear()
        self.lsr_symbols.clear()

def deduplicate_batch(batch):
    seen = set()
    unique_batch = []
    for record in batch:
        key = (record.get('ts'), record.get('symbol'))
        if key not in seen and all(k is not None for k in key):
            seen.add(key)
            unique_batch.append(record)
    return unique_batch

#================== PROCESS ====================================
def process_data(raw_data, base_symbol, data_type):
    """Process raw data and expand 5m to 1m intervals.
    Returns (immediate_records, future_records)"""
    try:
        if not raw_data:
            return [], []

        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_min = now - (now % 60000)  # Floor to current minute
        
        immediate = []
        future = []
        
        for item in raw_data:
            ts = int(item['timestamp'])
            value = None

            if data_type == 'oi':
                value = float(item['sumOpenInterestValue'])
            elif data_type == 'lsr':
                value = float(item['longShortRatio'])

            if value is not None:
                for i in range(5):  # 5m to 1m expansion
                    minute_ts = ts + (i * 60 * 1000)
                    record = {"ts": minute_ts, "symbol": base_symbol}
                    
                    if data_type == 'oi':
                        record["oi"] = value
                    elif data_type == 'lsr':
                        record["lsr"] = value
                    
                    # Split into immediate (<=current) and future (>current)
                    if minute_ts <= current_min:
                        immediate.append(record)
                    else:
                        future.append(record)
        
        return deduplicate_batch(immediate), future
    except Exception as e:
        print(f"Processing error: {e}")
        return [], []

#================== G A P ====================================
async def check_data_gap(db_manager, lookback_minutes=20):
    """Check for gaps in OI and LSR data using UTC time."""
    try:
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        query = """
        SELECT MAX(ts)
        FROM perp_data
        WHERE (oi IS NOT NULL OR lsr IS NOT NULL)
        """
        result = await asyncio.to_thread(db_manager.execute_query, query, fetch="one")
        gap_ts = result[0] if result and result[0] else 0
        gap_minutes = (now - gap_ts) // 60000 if gap_ts else 0

        if gap_minutes > lookback_minutes:
            message = f"🔍oi/lsr Gap Check: {gap_minutes} minutes gap in DB."
            await log_error(db_manager, SCRIPT_NAME, "Gap Detect", message)
    except Exception as e:
        error_msg = f"Gap detection failed: {str(e)}"
        await log_error(db_manager, SCRIPT_NAME, "Gap Check Error", error_msg)
        print(f"{Fore.RED}⚠️ {error_msg}{Fore.RESET}")

#================== CACHE RELEASE ====================================
async def release_cached_records(db_manager, state):
    """Release cached records whose timestamp has arrived."""
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    current_min = now - (now % 60000)
    
    ready_records = [r for r in state.cache if r['ts'] <= current_min]
    state.cache = [r for r in state.cache if r['ts'] > current_min]
    
    if ready_records:
        # Separate OI and LSR
        oi_records = [r for r in ready_records if 'oi' in r]
        lsr_records = [r for r in ready_records if 'lsr' in r]
        
        if oi_records:
            try:
                await asyncio.to_thread(db_manager.insert_batch_data, deduplicate_batch(oi_records))
                state.oi_symbols.update(record['symbol'] for record in oi_records)
            except Exception as e:
                await log_error(db_manager, SCRIPT_NAME, "Cache Release", f"OI insert error: {e}")
        
        if lsr_records:
            try:
                await asyncio.to_thread(db_manager.insert_batch_data, deduplicate_batch(lsr_records))
                state.lsr_symbols.update(record['symbol'] for record in lsr_records)
            except Exception as e:
                await log_error(db_manager, SCRIPT_NAME, "Cache Release", f"LSR insert error: {e}")

#==============================================================
async def fetch_binance_data(session, endpoint_key, base_symbol):
    config = BINANCE_API_CONFIG[endpoint_key]
    config['params']['symbol'] = f"{base_symbol}USDT"
    async with session.get(config['url'], params=config['params']) as response:
        if response.status == 200:
            return await response.json()
        else:
            return None

async def poll_binance_data(session, db_manager, state):
    # First, release any cached records that are now due
    await release_cached_records(db_manager, state)
    
    # Fetch new data
    tasks = []
    for symbol in BASE_SYMBOLS:
        tasks.append(asyncio.create_task(fetch_binance_data(session, 'oi', symbol)))
        tasks.append(asyncio.create_task(fetch_binance_data(session, 'lsr', symbol)))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    immediate_oi = []
    immediate_lsr = []
    
    for i, result in enumerate(results):
        if isinstance(result, list) and len(result) > 0:
            if i % 2 == 0:  # OI results
                imm, fut = process_data(result, BASE_SYMBOLS[i//2], 'oi')
                immediate_oi.extend(imm)
                state.cache.extend(fut)  # Cache future records
            else:  # LSR results
                imm, fut = process_data(result, BASE_SYMBOLS[(i-1)//2], 'lsr')
                immediate_lsr.extend(imm)
                state.cache.extend(fut)  # Cache future records

    # Insert immediate OI data
    if immediate_oi:
        final_oi = deduplicate_batch(immediate_oi)
        try:
            await asyncio.to_thread(db_manager.insert_batch_data, final_oi)
            state.oi_symbols.update(record['symbol'] for record in final_oi)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", f"Insert error for OI: {e}")

    # Insert immediate LSR data
    if immediate_lsr:
        final_lsr = deduplicate_batch(immediate_lsr)
        try:
            await asyncio.to_thread(db_manager.insert_batch_data, final_lsr)
            state.lsr_symbols.update(record['symbol'] for record in final_lsr)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", f"Insert error for LSR: {e}")

async def main():
    db_manager = DBManager()
    if not db_manager.conn:
        return

    state = ScriptState()
    await log_status(db_manager, SCRIPT_NAME, "Started", f"{STATUS_COLOR}🚦 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols.{RESET}")
    await check_data_gap(db_manager)

    async with aiohttp.ClientSession() as session:
        try:
            while True:
                try:
                    await poll_binance_data(session, db_manager, state)
                    # Report unique symbol count
                    oi_count = len(state.oi_symbols)
                    lsr_count = len(state.lsr_symbols)
                    print(f"{STATUS_COLOR}🚥 {SCRIPT_DEF} | OI: {oi_count} | LSR: {lsr_count}{RESET}")
                    state.reset_counts()
                except Exception as e:
                    await log_error(db_manager, SCRIPT_NAME, "Internal", f"Polling error: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            print(f"{Fore.YELLOW}🚨 Main loop cancelled gracefully.{Fore.RESET}")
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", f"Main loop error: {e}")

if __name__ == "__main__":
    asyncio.run(main())