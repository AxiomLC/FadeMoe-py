# apis/2oi_lsr_c.py. rev:5Dec 2025 ver:1;
# ver 2 correcting gap detect
import asyncio
import signal
import aiohttp
from datetime import datetime, timezone
from colorama import Fore, init as colorama_init
from back.api_utils import log_status, log_error
from back.dbsetup import DBManager
from back.perp_input import BASE_SYMBOLS
# Initialize colorama for console coloring
colorama_init()

def shutdown_handler(signum, frame):
    print(f"{Fore.YELLOW}🚨 Shutting down gracefully...{Fore.RESET}")
    asyncio.create_task(shutdown())

async def shutdown():
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"{Fore.YELLOW}🚨 Shutdown complete.{Fore.RESET}")
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
#==========================================================
# Script configuration
SCRIPT_NAME = "2oi_lsr_c.py"
SCRIPT_DEF = "Real-time OI & LSR Poll"
STATUS_COLOR = Fore.LIGHTGREEN_EX  # Consistent with template
RESET = Fore.RESET
HEARTBEAT_INTERVAL = 60  # Consistent naming; matches template

# Binance API configuration (from JS references)
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

# State for tracking, similar to template
class ScriptState:
    def __init__(self):
        self.oi_inserts = 0
        self.lsr_inserts = 0

    def reset_counts(self):
        self.oi_inserts = 0
        self.lsr_inserts = 0  # Keep it simple

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
    """Process raw data and insert 0 for unchanged OI if needed."""
    try:
        if not raw_data:
            return []

        processed = []
        for item in raw_data:
            ts = int(item['timestamp'])
            value = None

            if data_type == 'oi':
                value = float(item['sumOpenInterestValue'])
                # Insert 0 if OI hasn't changed
                if value == 0:
                    value = 0.0
            elif data_type == 'lsr':
                value = float(item['longShortRatio'])

            if value is not None:
                temp_processed = []
                seen_temporary = set()
                for i in range(5):  # 5m to 1m expansion
                    minute_ts = ts + (i * 60 * 1000)
                    record = None
                    if data_type == 'oi':
                        record = {"ts": minute_ts, "symbol": base_symbol, "oi": value}
                    elif data_type == 'lsr':
                        record = {"ts": minute_ts, "symbol": base_symbol, "lsr": value}
                    key = (record.get('ts'), record.get('symbol'))
                    if key not in seen_temporary:
                        seen_temporary.add(key)
                        temp_processed.append(record)
                processed.extend(deduplicate_batch(temp_processed))
        return processed
    except Exception as e:
        print(f"Processing error: {e}")
        return []
#=================== GAP ===============================================
async def check_data_gap(db_manager, lookback_minutes=20):
    """Check for gaps in OI and LSR data using UTC time."""
    try:
        # Get current UTC time in milliseconds
        now = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Fetch most recent timestamp with any OI or LSR data
        query = """
        SELECT MAX(ts)
        FROM perp_data
        WHERE (oi IS NOT NULL OR lsr IS NOT NULL)
        """
        result = await asyncio.to_thread(db_manager.execute_query, query, fetch="one")

        # Calculate gap in minutes using UTC time
        gap_ts = result[0] if result and result[0] else 0
        gap_minutes = (now - gap_ts) // 60000 if gap_ts else 0
        
        # Debug output
        print(f"{Fore.YELLOW}🔍 Gap Check: {gap_minutes} minutes gap detected (threshold: {lookback_minutes}){Fore.RESET}")

        # Log and display gap message if needed
        if gap_minutes > lookback_minutes:
            message = f"oi / lsr has {gap_minutes} minutes or more gap in DB."
            await log_error(db_manager, SCRIPT_NAME, "Gap Detect", message)

    except Exception as e:
        error_msg = f"Gap detection failed: {str(e)}"
        await log_error(db_manager, SCRIPT_NAME, "Gap Check Error", error_msg)
        print(f"{Fore.RED}⚠️ {error_msg}{Fore.RESET}")

#====================================================================
async def fetch_binance_data(session, endpoint_key, base_symbol):
    config = BINANCE_API_CONFIG[endpoint_key]
    config['params']['symbol'] = f"{base_symbol}USDT"
    async with session.get(config['url'], params=config['params']) as response:
        if response.status == 200:
            return await response.json()
        else:
            return None

async def poll_binance_data(session, db_manager, state):
    tasks = []
    for symbol in BASE_SYMBOLS:
        tasks.append(asyncio.create_task(fetch_binance_data(session, 'oi', symbol)))
        tasks.append(asyncio.create_task(fetch_binance_data(session, 'lsr', symbol)))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    processed_oi = []
    processed_lsr = []
    for i, result in enumerate(results):
        if isinstance(result, list) and len(result) > 0:
            if i % 2 == 0:  # OI results
                oi_data = process_data(result, BASE_SYMBOLS[i//2], 'oi')
                processed_oi.extend(oi_data)
            else:  # LSR results
                lsr_data = process_data(result, BASE_SYMBOLS[(i-1)//2], 'lsr')
                processed_lsr.extend(lsr_data)

    if processed_oi:
        final_oi = deduplicate_batch(processed_oi)
        try:
            await asyncio.to_thread(db_manager.insert_batch_data, final_oi)
            state.oi_inserts += len(final_oi)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", f"Insert error for OI: {e}")

    if processed_lsr:
        final_lsr = deduplicate_batch(processed_lsr)
        try:
            await asyncio.to_thread(db_manager.insert_batch_data, final_lsr)
            state.lsr_inserts += len(final_lsr)
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", f"Insert error for LSR: {e}")
#=======================================================
async def main():
    db_manager = DBManager()
    if not db_manager.conn:
        return

    state = ScriptState()
    await log_status(db_manager, SCRIPT_NAME, "Started", f"🚀 {STATUS_COLOR}Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols.{RESET}")

    # Check for gaps before starting
    await check_data_gap(db_manager)

    async with aiohttp.ClientSession() as session:
        try:
            while True:
                try:
                    await poll_binance_data(session, db_manager, state)
                    total_oi = state.oi_inserts
                    total_lsr = state.lsr_inserts
                    print(f"{STATUS_COLOR}📊 {SCRIPT_DEF} OI: {total_oi} | LSR: {total_lsr}{RESET}")
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