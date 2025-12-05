# apis/2web_ohlc_lqtv_c.py. rev:2Dec2025 ver:8; WebSocket OHLCV, Liq, Taker Volume.
# TBV/TSV uses volume weighting to match backfill (1z_rsi_tv-h.py), pending review for value vs. price bias method.
# and Taker Volume (trade aggregation with OHLCV volume-weighted TBV/TSV distribution). It processes confirmed 1-minute candles,
# aggregates trade and liquidation events into minute buckets, and ensures thread-safe database insertions.
# Future consideration: A master-api.py may overlap startup of this 'c' script milliseconds before 'h' backfill completion for seamless data continuity.
# Line 381 "await check_data_gap" to change gap detect
import asyncio
import signal
import json
import websockets
from collections import defaultdict
from datetime import datetime, timezone
from colorama import Fore, init as colorama_init
from back.api_utils import log_status, log_error
from back.dbsetup import DBManager
from back.perp_input import BASE_SYMBOLS
# Initialize colorama for console coloring
colorama_init()

def shutdown_handler(signum, frame):
    print(f"{YELLOW}🚨 Shutting down gracefully...{RESET}")
    asyncio.create_task(shutdown())

async def shutdown():
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"{YELLOW}🚨 Shutdown complete.{RESET}")
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
#==========================================================
# Script configuration
SCRIPT_NAME = "2web_ohlc_lqtv_c.py"
SCRIPT_DEF = "Real Time OHLCV, TV, LQ Poll"
STATUS_COLOR = Fore.LIGHTGREEN_EX; RESET = Fore.RESET; YELLOW = Fore.YELLOW; RED = Fore.RED
HEARTBEAT_INTERVAL = 60  # Console heartbeat every minute
TV_FLUSH_INTERVAL = 5; LQ_FLUSH_INTERVAL = 15  # Check for completed buckets every__s
MT_SYMBOLS = ["ETH", "BTC", "XRP", "SOL"]; MT_SYMBOL = "MT"
BATCH_SIZE = 5; BATCH_DELAY = 0.2  # Stagger and reduce delay
RETRY_INITIAL_DELAY = 5  # Initial retry delay for WebSocket failures
RETRY_MAX_DELAY = 60; RETRY_MAX_ATTEMPTS = 5

# Binance WebSocket configuration
BINANCE_WS_BASE = "wss://fstream.binance.com/ws"

# State tracking for data aggregation and MT calculation
mt_latest_data = {sym: None for sym in MT_SYMBOLS}  # Latest OHLCV for MT
tv_buckets = defaultdict(lambda: defaultdict(dict))  # TV: {symbol: {ts: {tbv_total, tsv_total, window_start}}}
lq_buckets = defaultdict(lambda: defaultdict(dict))  # LQ: {symbol: {ts: {lql_sum, lqs_sum}}}

# Connection status tracking for logging
connected_flags = {"OHLCV": False, "LQ": False, "TV": False}
connected_logged = False

class ScriptState:
    def __init__(self):
        self.ohlcv_inserts = 0
        self.lq_inserts = 0
        self.tv_inserts = 0
        self.mt_inserts = 0

    def reset_counts(self):
        """Reset counts for the next heartbeat interval to show non-cumulative data."""
        self.ohlcv_inserts = 0
        self.lq_inserts = 0
        self.tv_inserts = 0
        self.mt_inserts = 0

def floor_to_minute(ts_ms):
    """Floor timestamp to the nearest minute boundary."""
    return int(ts_ms / 60000) * 60000
# ==================  G A P ==========================================================
async def check_data_gap(db_manager, lookback_minutes=20):
    """Check for gaps in OHLCV and TV data using UTC time."""
    try:
        # Get current UTC time in milliseconds
        now = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Fetch most recent timestamp with any OHLCV or TV data
        query = """
        SELECT MAX(ts)
        FROM perp_data
        WHERE (o IS NOT NULL OR h IS NOT NULL OR l IS NOT NULL OR c IS NOT NULL OR v IS NOT NULL OR
               tbv IS NOT NULL OR tsv IS NOT NULL)
        """
        result = await asyncio.to_thread(db_manager.execute_query, query, fetch="one")

        # Calculate gap in minutes using UTC time
        gap_ts = result[0] if result and result[0] else 0
        gap_minutes = (now - gap_ts) // 60000 if gap_ts else 0
        
        # Debug output
        print(f"{YELLOW}🔍 Gap Check: {gap_minutes} minutes gap detected (threshold: {lookback_minutes}){RESET}")

        # Log and display gap message if needed
        if gap_minutes > lookback_minutes:
            message = f"ohlcv / tv has {gap_minutes} minutes or more gap in DB."
            await log_error(db_manager, SCRIPT_NAME, "Gap Detect", message)

    except Exception as e:
        error_msg = f"Gap detection failed: {str(e)}"
        await log_error(db_manager, SCRIPT_NAME, "Gap Check Error", error_msg)
        print(f"{RED}⚠️ {error_msg}{RESET}")
#======================================================================
async def check_all_connected(db_manager):
    """Check if all WebSocket types are connected and log once."""
    global connected_logged
    if not connected_logged and all(connected_flags.values()):
        connected_logged = True
        message = f"{STATUS_COLOR}🚦 Websocket Collector connected; streaming started.{RESET}"
        await log_status(db_manager, SCRIPT_NAME, "Running", message)
#===========================================================
async def compute_mt_record(db_manager, current_ts):
    """Compute Market Trend (MT) record if all symbols have data up to current_ts."""
    total_o = total_h = total_l = total_c = total_v = 0
    count = 0
    for sym in MT_SYMBOLS:
        latest = mt_latest_data.get(sym)
        if latest and latest["ts"] <= current_ts and all(key in latest for key in ["o", "h", "l", "c", "v"]) and latest["o"] > 0:
            total_o += latest["o"]
            total_h += latest["h"]
            total_l += latest["l"]
            total_c += latest["c"]
            total_v += latest["v"]
            count += 1
    if count == len(MT_SYMBOLS):
        record = {
            "ts": current_ts,
            "symbol": MT_SYMBOL,
            "o": total_o / count,
            "h": total_h / count,
            "l": total_l / count,
            "c": total_c / count,
            "v": total_v / count  # Averaged volume as per correction
        }
        try:
            await asyncio.to_thread(db_manager.insert_batch_data, [record])  # Overwrites on conflict (ts, symbol)
            return record
        except Exception as e:
            await log_error(db_manager, SCRIPT_NAME, "Internal", "MT Insert Error", f"Failed to insert MT record: {e}")
            print(f"{RED}❌ MT insert failed: {e}{RESET}")
    return None

async def get_tv_bucket(db_manager, symbol, timestamp):
    """Get or create a 1-minute volume bucket for Taker Volume."""
    window_start = floor_to_minute(timestamp)
    if window_start not in tv_buckets[symbol]:
        tv_buckets[symbol][window_start] = {
            "tbv_total": 0.0,
            "tsv_total": 0.0,
            "window_start": window_start
        }
    return tv_buckets[symbol][window_start]

async def flush_tv_bucket(db_manager, state, symbol, window_start):
    """Flush a completed Taker Volume bucket using OHLCV volume weighting."""
    if symbol not in tv_buckets or window_start not in tv_buckets[symbol]:
        return
    bucket = tv_buckets[symbol][window_start]
    if bucket["tbv_total"] == 0 and bucket["tsv_total"] == 0:
        return

    try:
        # Query OHLCV for the minute to determine volume weighting
        query = "SELECT ts, v::numeric FROM perp_data WHERE symbol = %s AND ts = %s AND v IS NOT NULL"
        ohlcv_rows = await asyncio.to_thread(db_manager.execute_query, query, (symbol, window_start), fetch="all")
        tbv, tsv = 0.0, 0.0
        if ohlcv_rows and len(ohlcv_rows) > 0 and ohlcv_rows[0][1] is not None:
            # Since it's a single minute, total volume is just the value for that minute
            total_volume = float(ohlcv_rows[0][1])
            if total_volume > 0:
                # Weighting is 100% for this minute since we're already at 1-minute granularity
                tbv = bucket["tbv_total"]
                tsv = bucket["tsv_total"]
            else:
                # Fallback if volume is zero or invalid
                tbv = bucket["tbv_total"] / 1.0
                tsv = bucket["tsv_total"] / 1.0
        else:
            # Fallback if no OHLCV data available for this minute
            tbv = bucket["tbv_total"] / 1.0
            tsv = bucket["tsv_total"] / 1.0

        record = {
            "ts": window_start,
            "symbol": symbol,
            "tbv": tbv,
            "tsv": tsv
        }
        await asyncio.to_thread(db_manager.insert_batch_data, [record])  # Overwrites on conflict (ts, symbol)
        state.tv_inserts += 1
        del tv_buckets[symbol][window_start]
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "TV Flush Error", f"Failed for {symbol} at {window_start}: {e}")
        print(f"{RED}❌ TV flush failed for {symbol}: {e}{RESET}")

async def flush_lq_bucket(db_manager, state, symbol, window_start):
    """Flush a completed Liquidation bucket."""
    if symbol not in lq_buckets or window_start not in lq_buckets[symbol]:
        return
    bucket = lq_buckets[symbol][window_start]
    if bucket.get("lql_sum", 0) == 0 and bucket.get("lqs_sum", 0) == 0:
        return

    try:
        record = {
            "ts": window_start,
            "symbol": symbol,
            "lql": bucket["lql_sum"],
            "lqs": bucket["lqs_sum"]
        }
        await asyncio.to_thread(db_manager.insert_batch_data, [record])  # Overwrites on conflict (ts, symbol)
        state.lq_inserts += 1
        del lq_buckets[symbol][window_start]
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "LQ Flush Error", f"Failed for {symbol} at {window_start}: {e}")
        print(f"{RED}❌ LQ flush failed for {symbol}: {e}{RESET}")

async def periodic_flush(db_manager, state):
    """Periodically flush completed buckets for TV and LQ."""
    while True:
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_window = floor_to_minute(now)
        # TV flush for completed minutes
        for symbol in list(tv_buckets.keys()):
            for window_start in list(tv_buckets[symbol].keys()):
                if window_start < current_window:
                    await flush_tv_bucket(db_manager, state, symbol, window_start)
        # LQ flush for completed minutes (slightly longer delay)
        threshold = now - 60000  # 1 minute ago
        for symbol in list(lq_buckets.keys()):
            for window_start in list(lq_buckets[symbol].keys()):
                if window_start < threshold:
                    await flush_lq_bucket(db_manager, state, symbol, window_start)
        await asyncio.sleep(TV_FLUSH_INTERVAL if TV_FLUSH_INTERVAL < LQ_FLUSH_INTERVAL else LQ_FLUSH_INTERVAL)

async def periodic_heartbeat(state):
    """Periodic console heartbeat to confirm script is running with non-cumulative counts since last heartbeat."""
    while True:
        total_ohlcv = state.ohlcv_inserts
        total_lq = state.lq_inserts
        total_tv = state.tv_inserts #total_mt = state.mt_inserts
        print(f"{STATUS_COLOR}📊 {SCRIPT_DEF} OHLCV: {total_ohlcv}| LQ: {total_lq}| TV: {total_tv}{RESET}")
        state.reset_counts()
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def connect_websocket(db_manager, state, ws_type, symbol, retry_count=0):
    """Connect to a Binance WebSocket stream with retry logic."""
    if ws_type == "OHLCV":
        stream = f"{symbol.lower()}usdt@kline_1m"
    elif ws_type == "LQ":
        stream = f"{symbol.lower()}usdt@forceOrder"
    elif ws_type == "TV":
        stream = f"{symbol.lower()}usdt@aggTrade"
    else:
        return

    url = f"{BINANCE_WS_BASE}/{stream}"
    try:
        async with websockets.connect(url) as ws:
            if not connected_flags[ws_type]:
                connected_flags[ws_type] = True
                await check_all_connected(db_manager)
            while True:
                message = await ws.recv()
                data = json.loads(message)
                if ws_type == "OHLCV":
                    await process_ohlcv(db_manager, state, symbol, data)
                elif ws_type == "LQ":
                    await process_lq(db_manager, state, symbol, data)
                elif ws_type == "TV":
                    await process_tv(db_manager, state, symbol, data)
    except Exception as e:
        retry_count += 1
        if retry_count <= RETRY_MAX_ATTEMPTS:
            delay = min(RETRY_INITIAL_DELAY * (2 ** (retry_count - 1)), RETRY_MAX_DELAY)
            print(f"{YELLOW}⚠ WebSocket {ws_type} error for {symbol}: {e}. Retrying in {delay}s ({retry_count}/{RETRY_MAX_ATTEMPTS}){RESET}")
            await asyncio.sleep(delay)
            await connect_websocket(db_manager, state, ws_type, symbol, retry_count)
        else:
            await log_error(db_manager, SCRIPT_NAME, "WebSocket", f"{ws_type} Failure for {symbol}", f"Failed after {retry_count} retries: {e}")
            print(f"{RED}❌ Max retries reached for {ws_type} {symbol}: {e}{RESET}")

async def process_ohlcv(db_manager, state, symbol, data):
    """Process OHLCV WebSocket data."""
    try:
        k = data.get("k", {})
        if not k.get("x", False):  # Only process confirmed candles
            return
        ts = int(k.get("t", 0))
        record = {
            "ts": ts,
            "symbol": symbol,
            "o": float(k.get("o", 0)),
            "h": float(k.get("h", 0)),
            "l": float(k.get("l", 0)),
            "c": float(k.get("c", 0)),
            "v": float(k.get("q", 0))
        }
        await asyncio.to_thread(db_manager.insert_batch_data, [record])  # Overwrites on conflict (ts, symbol)
        state.ohlcv_inserts += 1
        if symbol in MT_SYMBOLS:
            mt_latest_data[symbol] = {
                "ts": ts,
                "o": record["o"],
                "h": record["h"],
                "l": record["l"],
                "c": record["c"],
                "v": record["v"]
            }
            mt_record = await compute_mt_record(db_manager, ts)
            if mt_record:
                state.mt_inserts += 1
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "OHLCV Parse Error", f"Failed for {symbol}: {e}")
        print(f"{RED}❌ OHLCV parse error for {symbol}: {e}{RESET}")

async def process_lq(db_manager, state, symbol, data):
    """Process Liquidation WebSocket data."""
    try:
        if data.get("e") != "forceOrder":
            return
        o = data.get("o", {})
        ts = int(o.get("T", 0))
        side = "short" if o.get("S", "") == "BUY" else "long"
        price = float(o.get("p", 0))
        qty = float(o.get("q", 0))
        usd_value = price * qty
        window_start = floor_to_minute(ts)
        if window_start not in lq_buckets[symbol]:
            lq_buckets[symbol][window_start] = {"lql_sum": 0.0, "lqs_sum": 0.0}
        bucket = lq_buckets[symbol][window_start]
        if side == "long":
            bucket["lql_sum"] += usd_value
        else:
            bucket["lqs_sum"] += usd_value
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "LQ Parse Error", f"Failed for {symbol}: {e}")
        print(f"{RED}❌ LQ parse error for {symbol}: {e}{RESET}")

async def process_tv(db_manager, state, symbol, data):
    """Process Taker Volume WebSocket data."""
    try:
        ts = int(data.get("T", 0))
        is_buy = not data.get("m", True)  # m=False means taker buy
        volume = float(data.get("q", 0))
        bucket = await get_tv_bucket(db_manager, symbol, ts)
        if is_buy:
            bucket["tbv_total"] += volume
        else:
            bucket["tsv_total"] += volume
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "TV Parse Error", f"Failed for {symbol}: {e}")
        print(f"{RED}❌ TV parse error for {symbol}: {e}{RESET}")

async def main():
    """Main execution to start WebSocket connections and background tasks."""
    db_manager = DBManager()
    if not db_manager.conn:
        error_msg = f"❌ Database connection failed. Aborting."
        print(f"{RED}{error_msg}{RESET}")
        await log_error(db_manager, SCRIPT_NAME, "Database", "Connection Failure", error_msg)
        return

    state = ScriptState()
    start_message = f"🚦 {STATUS_COLOR}** Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols.{RESET}"
    await log_status(db_manager, SCRIPT_NAME, "Started", start_message)

    # Check for initial gaps in OHLCV and TV data
    try:
        await asyncio.sleep(2)  # Small delay to ensure database is ready
        await check_data_gap(db_manager, lookback_minutes=30)
    except Exception as e:
        error_msg = f"Gap detection failed: {e}"
        print(f"{RED}❌ {error_msg}{RESET}")
        await log_error(db_manager, SCRIPT_NAME, "Gap Detection", "Failed to check initial gaps", str(e))

    # Start background tasks, ensuring startup message appears first
    asyncio.create_task(periodic_flush(db_manager, state))
    # Add longer delay to heartbeat to ensure connection messages display first
    await asyncio.sleep(10)
    asyncio.create_task(periodic_heartbeat(state))

    # Stagger WebSocket connections in batches for fast startup
    for i in range(0, len(BASE_SYMBOLS), BATCH_SIZE):
        batch = BASE_SYMBOLS[i:i + BATCH_SIZE]
        tasks = []
        for symbol in batch:
            tasks.extend([
                asyncio.create_task(connect_websocket(db_manager, state, "OHLCV", symbol)),
                asyncio.create_task(connect_websocket(db_manager, state, "LQ", symbol)),
                asyncio.create_task(connect_websocket(db_manager, state, "TV", symbol))
            ])
        await asyncio.sleep(BATCH_DELAY)

    # Keep the script running
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour, kept alive by background tasks
    except asyncio.CancelledError:
        stop_message = f"🛑 {SCRIPT_DEF} smoothly stopped."
        await log_status(db_manager, SCRIPT_NAME, "Stopped", stop_message)
        db_manager.close_connection()
    except Exception as e:
        await log_error(db_manager, SCRIPT_NAME, "Internal", "Main loop error", f"Unhandled exception: {e}")
        print(f"{RED}❌ Main loop error: {e}{RESET}")

if __name__ == "__main__":
    asyncio.run(main())