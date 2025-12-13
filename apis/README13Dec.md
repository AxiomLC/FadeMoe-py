# Database Architecture: COPY-Stage Pattern for Concurrent API Ingestion

## Overview

This system uses a **COPY-stage-merge architecture** designed for high-throughput, conflict-free data ingestion from multiple concurrent API scripts. The core principle is **non-destructive merging**: when different scripts write different parameters (OHLCV, RSI, TBV/TSV, PFR, OI, etc.) to the same timestamp-symbol pairs, existing data is **preserved** using PostgreSQL's `COALESCE` function. This allows 5-7+ API scripts to run simultaneously without race conditions, data loss, or lock contention. The system handles 600K+ records in ~100 seconds with proxy rotation for rate-limit circumvention.

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  CONCURRENT API SCRIPTS (run in parallel)                       │
│  • 1ohlcv_pfr_h.py    → fetches o,h,l,c,v + pfr                │
│  • 1z_rsi_tv_h.py     → calculates RSI + fetches TBV/TSV       │
│  • 1oi_lsr_h.py       → fetches Open Interest + Long/Short     │
│  • 1liq_h.py          → fetches Liquidations (lql/lqs)         │
│  (Each uses proxy rotation to avoid rate limits)                │
└────────────────┬────────────────────────────────────────────────┘
                 │ All call: await db.merge_data_to_stage(batch)
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: IN-MEMORY MERGE (merge_data_to_stage)                 │
│  • Flattens all incoming batches into single list               │
│  • Groups by (ts, symbol) key                                   │
│  • Preserves non-NULL values: if col exists, keep it           │
│  • Deduplicates: one record per (ts, symbol) with all params   │
└────────────────┬────────────────────────────────────────────────┘
                 │ Lock acquired (prevents staging table conflicts)
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: BULK COPY TO STAGING (via PostgreSQL COPY command)    │
│  • Chunks data into 4000-row batches                            │
│  • Uses psycopg2's COPY FROM for 100x faster inserts           │
│  • Writes to perp_data_stage (temporary holding table)          │
│  • No indexes = blazing fast writes                             │
└────────────────┬────────────────────────────────────────────────┘
                 │ Single atomic operation per lock cycle
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: COALESCE MERGE TO MAIN TABLE (_merge_stage_sync)      │
│  INSERT INTO perp_data SELECT * FROM perp_data_stage            │
│  ON CONFLICT (ts, symbol) DO UPDATE SET                         │
│    o   = COALESCE(EXCLUDED.o,   t.o),    ← NEW non-NULL wins   │
│    pfr = COALESCE(EXCLUDED.pfr, t.pfr),  ← Keep existing if NEW│
│    rsi = COALESCE(EXCLUDED.rsi, t.rsi),  ← is NULL             │
│    tbv = COALESCE(EXCLUDED.tbv, t.tbv),  ← NO DATA ERASURE     │
│  • EXCLUDED = new data from stage                               │
│  • t = existing row in perp_data                                │
│  • COALESCE picks first non-NULL value                          │
└────────────────┬────────────────────────────────────────────────┘
                 │ Stage table cleared for next batch
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  FINAL STATE: perp_data (TimescaleDB hypertable)               │
│  • Every (ts, symbol) has ALL parameters from different scripts│
│  • No NULLs overwrite existing data                             │
│  • 20-day retention policy (auto-cleanup old data)              │
│  • Indexed on ts DESC + symbol for fast queries                │
└─────────────────────────────────────────────────────────────────┘
```
##                     Key Design Decisions
**Why COALESCE?** Without it, when the RSI script inserts `{ts:X, symbol:Y, rsi:50}`, the merge would set `o=NULL, h=NULL, c=NULL...` and **erase** the OHLCV data written earlier by the OHLCV script. `COALESCE(EXCLUDED.o, t.o)` means "use the new value if it's not NULL, otherwise keep what's already there." This is the **critical** difference between data preservation and data loss.

**Why Staging Table?** Direct inserts to TimescaleDB hypertables with high concurrency cause lock contention. The staging table (perp_data_stage) has no primary key, no indexes, and accepts rapid COPY operations. The merge happens once per batch under an async lock, ensuring only one script writes to perp_data at a time while others queue safely. This architecture supports 20+ concurrent API scripts without deadlocks.

**Proxy Rotation & Rate Limits:** API scripts use `get_next_connection()` from `back.proxy` to cycle through 3+ proxy servers, distributing requests to avoid Binance's rate limits (2400 requests/minute per IP). Each script fetches different endpoints (klines, premiumIndex, takerlongshortRatio, openInterest) so combined throughput exceeds single-IP limits. The system logs which proxy fetched which data for debugging. Forward-fill logic (e.g., RSI/TV extending last known value to NOW) ensures real-time WebSocket scripts inherit a gapless database when they take over from REST backfills.
## ================================================================ ##
System Components & Orchestration
dbsetup.py is the foundation module that drops all existing tables/data on initialization and recreates the complete schema (perp_data, perp_metrics, staging tables, hypertables, indexes). It exports the DBManager class containing the critical merge_data_to_stage() and _merge_stage_sync() functions that enable conflict-free concurrent writes. 

All API scripts import BASE_SYMBOLS and DB_RETENTION_DAYS from back/perp_input.py so users can adjust database size and symbol lists in one central location, with changes flowing automatically through the entire application. Internal calculation scripts like RSI bypass this static list and query SELECT DISTINCT symbol FROM perp_data to dynamically discover which symbols have OHLCV data already populated by prerequisite scripts. 

Shared functionality (logging, symbol formatting, error handling) lives in back/api_utils.py to maintain DRY principles.
Code Consistency Standards: All backfill and real-time scripts follow a unified structure: imports → user-configurable settings → state management → helper functions → core logic → main orchestration. 

Each includes colored console output via colorama, heartbeat functions that print progress every 15 seconds without database writes, and detailed error handling with traceback logging to perp_errors table. Backfill scripts use asyncio.gather() for concurrent fetching, while real-time scripts use WebSocket streams with reconnection logic. Comments use section headers (# ===== SECTION =====) for visual navigation. This consistency ensures any developer (or AI) can understand script purpose and flow within 30 seconds of opening the file.
Master Orchestration Flow
┌──────────────────────────────────────────────────────────────────┐
│  master_api.py (ORCHESTRATOR)                                    │
│  Coordinates all data pipeline stages in dependency order        │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ├─► STAGE A: PRIMARY BACKFILLS (parallel execution)
             │   ├─ 1ohlcv_pfr_h.py    → Fetch OHLCV + PFR (20 days)
             │   ├─ 1oi_lsr_h.py       → Fetch OI + LSR (20 days)
             │   └─ 1liq_h.py          → Fetch Liquidations (20 days)
             │   ⚠️  These MUST complete before Stage B
             │
             ├─► STAGE B: DEPENDENT BACKFILLS (requires OHLCV data)
             │   ├─ 1z_rsi_tv_h.py     → Calculate RSI from perp_data.c
             │   │                       Fetch/weight TBV/TSV by volume
             │   │                       Auto-discovers symbols in DB
             │   └─ (other calculated params that need base data)
             │   ⚠️  Validates OHLCV completeness before running
             │
             ├─► STAGE C: METRICS BACKFILL (perp_metrics table)
             │   └─ backfill_metrics.py
             │          • Reads perp_data (all params)
             │          • Calculates _chg_1m, _chg_5m, _chg_10m columns
             │          • Writes to perp_metrics via COPY-stage pattern
             │          • Fills 20-day historical % change data
             │
             ├─► STAGE D: REAL-TIME INGESTION (continuous WebSocket)
             │   ├─ 2web_ohlc_lqtv_c.py
             │   ├─ 2oi_lsr_c.py       
             │   ├─ 2rsi_pfr_c.py 
             │   └─ (other real-time streams)
             │   🔄 Run indefinitely, update perp_data every 1-60s
             │
             └─► STAGE E: REAL-TIME METRICS (continuous calculation)
                 └─ calc_metrics.py (real-time mode)
                    • Monitors perp_data for new rows
                    • Calculates % changes on-the-fly
                    • Updates perp_metrics within seconds of new data
                    • Ensures trading algorithms have fresh signals
Dependency Chain: Stages A→B→C  complete sequentially with some overlap as dev deems, during initial backfill (cold start). Once perp_data and perp_metrics are populated, Stages D and E run continuously in parallel. If a real-time script crashes, master_api.py detects the gap and triggers the appropriate backfill script for that timespan before resuming live ingestion, maintaining database continuity.