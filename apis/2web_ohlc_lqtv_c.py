# apis/2web_ohlc_lqtv_c.py   rev:5Dec2025 ver:10
# Real-time OHLCV, Liquidations, Taker Volume
# Streamlined DB-based heartbeat (like oi_lsr)
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
colorama_init()

SCRIPT_NAME = "2web_ohlc_lqtv_c.py"
SCRIPT_DEF = "Real-Time OHLCV, Liq, TV"
GREEN = Fore.LIGHTGREEN_EX
YELLOW = Fore.YELLOW
RED = Fore.RED
RESET = Fore.RESET
HEARTBEAT_INTERVAL = 60
TV_FLUSH_INTERVAL = 5; LQ_FLUSH_INTERVAL = 15
MT_SYMBOLS = ["ETH", "BTC", "XRP", "SOL"]; MT_SYMBOL = "MT"

BATCH_SIZE = 5; BATCH_DELAY = 0.2; RETRY_INITIAL_DELAY = 5
RETRY_MAX_DELAY = 60; RETRY_MAX_ATTEMPTS = 5
BINANCE_WS_BASE = "wss://fstream.binance.com/ws"

mt_latest_data = {sym: None for sym in MT_SYMBOLS}
tv_buckets = defaultdict(lambda: defaultdict(dict))
lq_buckets = defaultdict(lambda: defaultdict(dict))

connected_flags = {"OHLCV": False, "LQ": False, "TV": False}
connected_logged = False
def floor_minute(ts):
    return int(ts / 60000) * 60000
    #==================================
def shutdown_handler(*_):
    try:
        asyncio.get_event_loop().create_task(shutdown())
    except:
        pass

async def shutdown():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()
signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
#================== GAP =========================================
async def check_data_gap(db):
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    q = """
        SELECT MAX(ts) FROM perp_data
        WHERE (o IS NOT NULL OR h IS NOT NULL OR l IS NOT NULL OR c IS NOT NULL OR v IS NOT NULL
               OR tbv IS NOT NULL OR tsv IS NOT NULL OR lql IS NOT NULL OR lqs IS NOT NULL)
    """
    try:
        r = await asyncio.to_thread(db.execute_query, q, fetch="one")
        gap_ts = r[0] or 0
        m = (now - gap_ts) // 60000 if gap_ts else 0
        print(f"{YELLOW}🔍ohlcv/tv Gap Check: {m} minutes gap detected{RESET}")
        if m > 30:
            await log_error(db, SCRIPT_NAME, "Gap Detect", f"gap={m}m")
    except Exception as e:
        print(f"{RED}Gap check error: {e}{RESET}")

#==================================================================
async def check_connected(db):
    global connected_logged
    if not connected_logged and all(connected_flags.values()):
        connected_logged = True
        await log_status(db, SCRIPT_NAME, "Running", "{GREEN}websockets connected{RESET}")
        print(f"{GREEN}🚦 Websocket Collector connected; streaming started.{RESET}")


async def compute_mt(db, ts):
    total = {"o":0,"h":0,"l":0,"c":0,"v":0}
    for sym in MT_SYMBOLS:
        d = mt_latest_data.get(sym)
        if not d or d["ts"] != ts:
            return
        for k in total:
            total[k] += d[k]
    count = len(MT_SYMBOLS)
    rec = {
        "ts": ts,
        "symbol": MT_SYMBOL,
        "o": total["o"]/count,
        "h": total["h"]/count,
        "l": total["l"]/count,
        "c": total["c"]/count,
        "v": total["v"]/count
    }
    try:
        await asyncio.to_thread(db.insert_batch_data, [rec])
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "MT Insert", str(e))


async def flush_tv(db, sym, w):
    b = tv_buckets[sym].get(w)
    if not b or (b["tbv_total"]==0 and b["tsv_total"]==0):
        tv_buckets[sym].pop(w, None)
        return
    rec = {"ts": w, "symbol": sym, "tbv": b["tbv_total"], "tsv": b["tsv_total"]}
    try:
        await asyncio.to_thread(db.insert_batch_data, [rec])
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "TV flush", str(e))
    finally:
        tv_buckets[sym].pop(w, None)


async def flush_lq(db, sym, w):
    b = lq_buckets[sym].get(w)
    if not b or (b["lql_sum"]==0 and b["lqs_sum"]==0):
        lq_buckets[sym].pop(w, None)
        return
    rec = {"ts": w, "symbol": sym, "lql": b["lql_sum"], "lqs": b["lqs_sum"]}
    try:
        await asyncio.to_thread(db.insert_batch_data, [rec])
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "LQ flush", str(e))
    finally:
        lq_buckets[sym].pop(w, None)


async def periodic_flush(db):
    while True:
        now = int(datetime.now(timezone.utc).timestamp()*1000)
        cw = floor_minute(now)
        for sym in list(tv_buckets.keys()):
            for w in list(tv_buckets[sym].keys()):
                if w < cw:
                    await flush_tv(db, sym, w)
        threshold = now - 60000
        for sym in list(lq_buckets.keys()):
            for w in list(lq_buckets[sym].keys()):
                if w < threshold:
                    await flush_lq(db, sym, w)
        await asyncio.sleep(min(TV_FLUSH_INTERVAL, LQ_FLUSH_INTERVAL))


async def periodic_heartbeat(db):
    # Track unique symbols with data in the current interval
    ohlcv_symbols = set()
    lq_symbols = set()
    tv_symbols = set()
    
    last_heartbeat = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    while True:
        try:
            # Get current time and calculate time window
            now = int(datetime.now(timezone.utc).timestamp() * 1000)
            time_window = now - (now % 60000)  # Align to minute boundary
            
            # Query for new data since last heartbeat
            q = """
                SELECT symbol, 
                       MAX(CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END) as has_ohlcv,
                       MAX(CASE WHEN lql IS NOT NULL OR lqs IS NOT NULL THEN 1 ELSE 0 END) as has_lq,
                       MAX(CASE WHEN tbv IS NOT NULL OR tsv IS NOT NULL THEN 1 ELSE 0 END) as has_tv
                FROM perp_data
                WHERE ts >= %s
                GROUP BY symbol
            """
            
            results = await asyncio.to_thread(db.execute_query, q, (last_heartbeat,), fetch="all")
            
            # Update our symbol sets
            for symbol, has_ohlcv, has_lq, has_tv in results:
                if has_ohlcv:
                    ohlcv_symbols.add(symbol)
                if has_lq:
                    lq_symbols.add(symbol)
                if has_tv:
                    tv_symbols.add(symbol)
            
            # Get the counts
            ohlcv_count = len(ohlcv_symbols)
            lq_count = len(lq_symbols)
            tv_count = len(tv_symbols)
            
            # Print the status
            print(f"{GREEN}🚥 {SCRIPT_DEF} OHLCV: {ohlcv_count} | LQ: {lq_count} | TV: {tv_count}{RESET}")
            
            # Reset for next interval
            if now - last_heartbeat >= 60000:  # If we've passed a full minute
                ohlcv_symbols.clear()
                lq_symbols.clear()
                tv_symbols.clear()
                last_heartbeat = time_window
                
        except Exception as e:
            print(f"{RED}Heartbeat error: {e}{RESET}")
            
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def connect_ws(db, t, sym, retry=0):
    if t=="OHLCV":
        stream=f"{sym.lower()}usdt@kline_1m"
    elif t=="LQ":
        stream=f"{sym.lower()}usdt@forceOrder"
    else:
        stream=f"{sym.lower()}usdt@aggTrade"

    url=f"{BINANCE_WS_BASE}/{stream}"
    try:
        async with websockets.connect(url) as ws:
            if not connected_flags[t]:
                connected_flags[t]=True
                await check_connected(db)
            while True:
                msg=await ws.recv()
                data=json.loads(msg)
                if t=="OHLCV":
                    await process_ohlcv(db, sym, data)
                elif t=="LQ":
                    await process_lq(db, sym, data)
                else:
                    await process_tv(sym, data)
    except Exception as e:
        retry+=1
        if retry<=RETRY_MAX_ATTEMPTS:
            d=min(RETRY_INITIAL_DELAY*(2**(retry-1)),RETRY_MAX_DELAY)
            await asyncio.sleep(d)
            await connect_ws(db, t, sym, retry)
        else:
            await log_error(db, SCRIPT_NAME, "WebSocket", f"{t}:{sym} {e}")

#=================== PROCESS ========================================
async def process_ohlcv(db, sym, d):
    try:
        k=d.get("k",{})
        if not k.get("x"):
            return
        ts=int(k["t"])
        rec={"ts":ts,"symbol":sym,
             "o":float(k["o"]), "h":float(k["h"]),
             "l":float(k["l"]), "c":float(k["c"]),
             "v":float(k["q"])}
        await asyncio.to_thread(db.insert_batch_data,[rec])

        if sym in MT_SYMBOLS:
            mt_latest_data[sym]={
                "ts":ts,"o":rec["o"],"h":rec["h"],
                "l":rec["l"],"c":rec["c"],"v":rec["v"]
            }
            await compute_mt(db, ts)
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "OHLCV", str(e))

async def process_lq(db, sym, d):
    try:
        if d.get("e")!="forceOrder":
            return
        o=d.get("o",{})
        ts=int(o["T"])
        price=float(o["p"])
        qty=float(o["q"])
        usd=price*qty
        side="short" if o.get("S")=="BUY" else "long"
        w=floor_minute(ts)
        b=lq_buckets[sym].setdefault(w,{"lql_sum":0.0,"lqs_sum":0.0})
        if side=="long":
            b["lql_sum"]+=usd
        else:
            b["lqs_sum"]+=usd
    except Exception as e:
        await log_error(db, SCRIPT_NAME, "LQ", str(e))

async def process_tv(sym, d):
    try:
        ts=int(d["T"])
        vol=float(d["q"])
        w=floor_minute(ts)
        b=tv_buckets[sym].setdefault(w,{"tbv_total":0.0,"tsv_total":0.0})
        if not d.get("m",True):
            b["tbv_total"]+=vol
        else:
            b["tsv_total"]+=vol
    except Exception:
        pass
#============================================================
async def main():
    db=DBManager()
    if not db.conn:
        print(f"{RED}DB connect fail{RESET}")
        return

    print(f"{GREEN}🚦 Starting {SCRIPT_DEF} for {len(BASE_SYMBOLS)} symbols.{RESET}")
    await log_status(db, SCRIPT_NAME, "Started", "{GREEN}start{RESET}")

    await asyncio.sleep(2)
    await check_data_gap(db)

    asyncio.create_task(periodic_flush(db))
    await asyncio.sleep(10)
    asyncio.create_task(periodic_heartbeat(db))

    for i in range(0,len(BASE_SYMBOLS),BATCH_SIZE):
        for sym in BASE_SYMBOLS[i:i+BATCH_SIZE]:
            asyncio.create_task(connect_ws(db,"OHLCV",sym))
            asyncio.create_task(connect_ws(db,"LQ",sym))
            asyncio.create_task(connect_ws(db,"TV",sym))
        await asyncio.sleep(BATCH_DELAY)

    try:
        while True:
            await asyncio.sleep(3600)
    except:
        await log_status(db, SCRIPT_NAME, "Stopped", "stop")
        db.close_connection()


if __name__=="__main__":
    asyncio.run(main())
