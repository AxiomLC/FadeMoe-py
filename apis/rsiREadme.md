The error asyncio.locks.Lock object ... is bound to a different event loop happens because you're calling async code (calculate_and_insert_rsi and process_and_insert_tv) inside a thread via asyncio.run(), which creates a new event loop — but some resources like your asyncio.Lock (probably in DBManager) are bound to the main event loop, causing conflicts.

Explanation:
Your DBManager likely has asyncio.Lock() instances.
When you run your async function inside a new thread via asyncio.run(), that function uses a different event loop.
The locks cannot be shared across event loops, hence the error.
How to fix and still parallelize CPU-heavy work safely?
Option 1: Avoid `asyncio.run()` inside threads. Use asyncio.to_thread() only for sync CPU blocking code, not for async functions.

Option 2: Refactor CPU-heavy parts to be sync functions without asyncio calls, so that you can safely offload these to threads.

Here’s what I suggest for your case:
Refactor TV processing CPU-heavy code (e.g., all volume weighting and record creation) into a synchronous helper function that receives data and returns the list of TV records.
Keep async DB calls (which use your DBManager’s locks) in the main async function.
Offload only the pure CPU-bound parts to threads.
Conceptual snippet:
# Synchronous function to do CPU heavy TV distribution calculation
def generate_tv_records_sync(raw_tv_data, ohlcv_map):
    all_tv_records = []
    last_valid_tbv = None
    last_valid_tsv = None

    # ... copy your existing CPU heavy volume weighting logic here ...

    return all_tv_records, last_valid_tbv, last_valid_tsv

# Async TV processing function
async def process_and_insert_tv(db_manager, symbol, raw_tv_data, state):
    if not raw_tv_data:
        state.no_tv_data_symbols.append(symbol)
        return

    # Fetch OHLCV as usual
    min_ts = int(raw_tv_data[0]['timestamp'])
    max_ts = int(raw_tv_data[-1]['timestamp']) + (5 * 60 * 1000)
    query = "SELECT ts, v::numeric FROM perp_data WHERE symbol = %s AND ts >= %s AND ts < %s AND v IS NOT NULL ORDER BY ts ASC"
    all_ohlcv = await db_manager.execute_query(query, (symbol, min_ts, max_ts), fetch="all")

    ohlcv_map = {int(row[0]): float(row[1]) for row in all_ohlcv} if all_ohlcv else {}

    # Offload CPU heavy calc to thread-safe sync function
    all_tv_records, last_valid_tbv, last_valid_tsv = await asyncio.to_thread(generate_tv_records_sync, raw_tv_data, ohlcv_map)

    # Forward-fill TV to NOW with last known values (async, cheap)
    if all_tv_records and last_valid_tbv is not None and last_valid_tsv is not None:
        now_ts = floor_to_minute(int(datetime.now(timezone.utc).timestamp() * 1000))
        last_record = max(all_tv_records, key=lambda x: x['ts'])
        last_ts = last_record['ts']
        current_ts = last_ts + 60000
        while current_ts <= now_ts:
            all_tv_records.append({"ts": current_ts, "symbol": symbol, "tbv": last_valid_tbv, "tsv": last_valid_tsv})
            current_ts += 60000

    # Insert with async DB call
    if all_tv_records:
        inserted = await db_manager.merge_data_to_stage(all_tv_records)
        state.records_inserted += inserted

        
Why this works better
You only offload pure CPU work with no asyncio calls to thread.
You keep async DB calls on main event loop (same loop as the locks).
Avoids event loop conflicts and lock binding errors.
If you’d like, I can generate this full refactor edit for your process_and_insert_tv function with a separate sync CPU-bound helper function. Would that help?