# apis/READMEapis.md. rev:2Dec 2025 ver:2; updated to reflect the complete suite of backfill scripts and final architecture.

## Introduction & Architecture Summary

This directory contains a suite of high-performance, asynchronous Python scripts designed for historical data backfilling. The core architecture is built on `asyncio` and `curl-cffi` for fast, concurrent network requests. The primary challenge solved in these scripts is the safe and efficient integration of synchronous database libraries (`psycopg2`) with the asynchronous event loop.

The final, stable architecture achieves this by ensuring all blocking I/O operations are run in a separate thread via `asyncio.to_thread`. This applies to all database writes performed by `dbsetup.py`'s `DBManager` and, crucially, to all database logging handled by the shared `api_utils.py` module. This prevents the event loop from ever stalling, allowing network-bound tasks and CPU-bound tasks to execute with maximum concurrency.

---

## Core Shared Components

All scripts are built upon a foundation of shared utilities from the `back/` folder:

*   **`back/api_utils.py`**: Provides the core, non-blocking logging functions `async def log_status()` and `async def log_error()`. These are the standard way to log script status and errors to both the console and the `perp_status`/`perp_errors` database tables without causing deadlocks.
*   **`back/proxy.py`**: Contains the `get_next_connection()` function, which manages a rotating pool of proxies (and the local IP) to distribute network load and avoid API rate limits.
*   **`back/dbsetup.py`**: The `DBManager` class provides the single, persistent database connection and the essential `insert_batch_data` method used for all database writes.

---

## Script Execution Flow (Condensed)

The scripts generally follow a "Fetch-All, Process-All" or a "Symbol-by-Symbol" parallel architecture. The fundamental, non-blocking pattern is as follows:

```plaintext
[main()]
 |
 +-> Creates [DBManager] and [AsyncSession]
 |
 +-> Gathers a list of tasks (e.g., one for each symbol)
 |
 +-> `asyncio.gather(*tasks)` runs all tasks concurrently
     |
     +--> [A Single Symbol Task]
          |
          +--> 1. FETCH: `await fetch_data()` makes non-blocking API calls.
          |
          +--> 2. PROCESS: `process_data()` transforms raw data in memory.
          |
          +--> 3. INSERT: `await asyncio.to_thread(db_manager.insert_batch_data, ...)`
                          This is the KEY. The slow database write is pushed
                          to a separate thread, allowing the main loop to continue.
```

---

## Script-Specific Details & Logic

While based on a template, several scripts have unique logic:

*   **`1ohlcv_pfr_h2.py`**: The primary template. Fetches fundamental OHLCV and Funding Rate data from Binance. As it is the longest-running script, it includes a "Final Loop" to fetch the most recent data and ensure freshness upon completion.

*   **`1oi_lsr_h.py`**: Fetches Open Interest and Long/Short Ratio from Binance. The API provides this data in 5-minute intervals. This script contains special logic in its `process_data` function to expand each 5-minute record into five identical 1-minute records for granular storage.

*   **`1z_lq_h.py`**: Fetches Liquidation data from the third-party **Coinalyze API**, not Binance. It requires a `COINALYZE_KEY` in the `.env` file and uses a special symbol format (e.g., `BTCUSDT_PERP.A`). The data is 1-minute native but can be sparse.

*   **`1z_rsi_tv-h.py`**: A hybrid script that performs both API fetches and local calculations.
    *   **RSI:** Does **not** fetch from an API. It reads existing OHLCV `close` prices from the local database, calculates RSI using the `pandas` library, and includes a special case to calculate an RSI for the averaged "MT" token.
    *   **Taker Volume (TV):** Fetches 5-minute TV data from a Binance API, but then reads 1-minute OHLCV data from the local database to perform a *weighted distribution* of the taker volume across the five 1-minute candles.

---

## Orchestration & Future Plans

**Note (2 Dec 2025):** The scripts in this suite are designed to be orchestrated by a master script (e.g., `master_api.py`). The intended execution order is to first run the foundational `1...` series scripts (`1ohlcv_pfr_h2.py`, `1oi_lsr_h.py`) to populate the database with raw data. Following that, the `1z...` series scripts (`1z_lq_h.py`, `1z_rsi_tv-h.py`) should be run, as they depend on or enrich the foundational data.

The next phase of development will involve creating `2...` series scripts. These will not be historical backfills, but will be designed for continuous, real-time data collection, likely using WebSockets.
