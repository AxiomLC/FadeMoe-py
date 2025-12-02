# API Scripts Documentation

**Revision Date:** 2025-12-01
Note (1 Dec 2025): For future scalability, if running many API scripts in parallel overwhelms the database, implementing a connection pooler like PgBouncer would be the next logical step to manage write contention 

## Introduction & Summary

This document outlines the architecture and best practices for the asynchronous API scripts used in this project, primarily focusing on `1ohlcv_pfr_h.py` as the template for future development. These scripts are designed for high-performance, concurrent fetching of historical market data from cryptocurrency exchanges like Binance. They leverage an asynchronous pipeline to efficiently handle hundreds of thousands of network requests and database insertions, utilizing a rotating pool of proxies to manage API rate limits and distribute the network load.

The core challenge and solution in these scripts revolve around integrating blocking (synchronous) database operations with a non-blocking (asynchronous) network I/O framework. The final, successful architecture completely isolates the application's logging from shared utilities, using a self-contained console logging system to prevent deadlocks. All synchronous database writes are executed in a separate thread pool via `asyncio.to_thread`, ensuring that the main `asyncio` event loop is never blocked. This results in a highly efficient pipeline where network fetching can proceed at maximum speed, while the slower database insertions are handled concurrently in the background without stalling the application. The use of `curl-cffi` provides a fast, Pythonic interface for making HTTP requests with JA3/TLS fingerprint impersonation, crucial for interacting with modern web APIs.

---

## Code Flow & Function Architecture

The script follows a clear, top-down asynchronous pipeline. The diagram below illustrates the flow of control and the purpose of each key function.

```plaintext
[main()]
 |
 +--> Creates a single, persistent [DBManager()] connection.
 |
 +--> Creates a [proxy_map] for simple log names (e.g., "Proxy 1").
 |
 +--> Spawns the [heartbeat()] task for periodic progress updates.
 |
 +--> Enters an [AsyncSession()] context for efficient, reused network connections.
      |
      | (Historical Backfill)
      +--> Calls [run_fetch_cycle()]
      |    |
      |    +--> Creates a list of tasks, one for each symbol, calling [fetch_and_process_symbol()]
      |    |
      |    +--> [asyncio.as_completed()] concurrently awaits the results from each symbol task.
      |         |
      |         +--> [fetch_and_process_symbol()]
      |              |
      |              +--> [asyncio.gather()] concurrently calls [fetch_data()] for both OHLCV and PFR.
      |              |    |
      |              |    +--> [fetch_data()]
      |              |         |
      |              |         +--> Enters a `while` loop for pagination.
      |              |         |
      |              |         +--> Calls [get_next_connection()] from the proxy utility.
      |              |         |
      |              |         +--> `await session.get()` to perform the non-blocking API request.
      |              |         |
      |              |         +--> Updates the `current_start_time` for the next page and loops.
      |              |         |
      |              |         +--> Returns a complete list of raw data for the API type.
      |              |
      |              +--> Receives raw data, calls [process_data()] to format it.
      |              |
      |              +--> Loops through the processed data in batches.
      |                   |
      |                   +--> `await asyncio.to_thread(db_manager.insert_batch_data, ...)`
      |                        This is the CRITICAL step that runs the blocking database
      |                        insert in a separate thread, preventing the script from stalling.
      |
      | (Final Loop)
      +--> Calls [run_fetch_cycle(is_final_loop=True)] to fetch "up-to-now" data.
      |
      +--> Calculates and inserts the final MT Token data.
      |
 +--> Cancels the heartbeat, prints final summary stats, and closes the database connection.
```