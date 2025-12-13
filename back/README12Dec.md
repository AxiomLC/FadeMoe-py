# DB Architecture Evolution: Migrating to COPY-Based Staging (Dec 12 2025)

## Overview
This document details the migration of the FadeMoe5 database layer from traditional row-by-row INSERTs to high-performance COPY-based staging, while maintaining full compatibility with existing scripts.

## Problem Statement
Original `dbsetup.py` used `insert_batch_data()` with server-side UPSERT logic, which:
- Performed per-row insertion with psycopg2's `execute_batch()`
- Handled 15 columns including OHLCV, PFR, LSR, RSI, etc.
- Caused significant slowdowns during backfills (1M+ records)

## NEW ARCHITECTURE: COPY → STAGE → MERGE

### Core Components

#### 1. **DBManager in `dbsetup2.py`**
- **ThreadedConnectionPool**: 3-25 connections for concurrent operations
- **Hypertable Support**: TimescaleDB with retention policies based on `DB_RETENTION_DAYS`
- **Complete Tables**: `perp_data`, `perp_metrics`, `perp_status`, `perp_errors`, `combo_algos`

#### 2. **COPY Staging Tables**
```sql
perp_data_stage       (schema matches perp_data, no constraints)
perp_metrics_stage    (schema matches perp_metrics, no constraints)
```

#### 3. **Batch Processing Flow**
```
API Scripts → [Batch Collection] → merge_data_to_stage() → 
    (internal deduplication) → _copy_to_stage_sync() → 
    perp_data_stage → merge_stage() → perp_data → clear_stage()
```

## Key Functions & Their Roles

### Setup Methods
```python
# Async wrapper sync methods for compatibility with api_utils.py
setup_database() → _setup_database_sync()
# Creates all tables, staging tables, hypertables with retention
```

### INSERTION PATHS

#### 1. **Legacy Compatibility Path** (still works)
```python
await db.insert_batch_data(rows, target="perp_data")
# Falls back to traditional execute_batch for emergency use
```

#### 2. **New Unified Staging Path** (RECOMMENDED)
```python
# PRIMARY entry point for API scripts
count = await db.merge_data_to_stage(
    olhcv_batch,     # from OHLCV API
    pfr_batch,       # from PFR API
    rsi_batch,       # from RSI calculation
    tv_batch,        # from TV API
    # ... any number of batches
)
```

**Features of `merge_data_to_stage`:**
- **Automatic deduplication** across batches by `(ts, symbol)`
- **Column preference logic**: Later non-NULL values override earlier NULLs
- **Batched processing**: Respects `BATCH_INSERT_SIZE` for memory management
- **No inter-batch conflicts**: Handles OHLCV arriving at different rates than PFR
- **Returns actual staged count**

### COPY/MERGE Core
```python
_copy_to_stage_sync()     # Actual psycopg2.copy_from execution with \N nulls
merge_stage()             # Main table upsert with ON CONFLICT DO UPDATE
clear_stage()             # TRUNCATE staging table
```

**NULL Handling Special Note:**
```python
# PostgreSQL COPY requires \N (backslash-N) for NULLs, not empty strings
cur.copy_from(buf, "perp_data_stage", null="\\N", sep="\t")
```

## API Script Migration Guide

### BEFORE (Old Pattern)
```python
# Separate staging for each API type
for batch in olhcv_batches:
    await db.copy_to_stage(batch)
await db.merge_stage()
await db.clear_stage()

for batch in pfr_batches:
    await db.copy_to_stage(batch)
await db.merge_stage()      # ❌ ERROR: Duplicate (ts, symbol) conflict
```

### AFTER (New Pattern)
```python
# Unified processing with automatic merging
for start in range(0, max_len, BATCH_INSERT_SIZE):
    olhcv_chunk = olhcv[start:start+BATCH_INSERT_SIZE]
    pfr_chunk = pfr[start:start+BATCH_INSERT_SIZE]
    rsi_chunk = rsi[start:start+BATCH_INSERT_SIZE]
    
    count = await db.merge_data_to_stage(olhcv_chunk, pfr_chunk, rsi_chunk)
    inserted += count

# Single merge at the end
if inserted > 0:
    await db.merge_stage()
    await db.clear_stage()
    total_records_inserted += inserted
```

### Benefits
1. **Performance**: COPY ~8,500-11,000 rows/sec vs INSERT ~200 rows/sec
2. **Multi-API Coexistence**: OHLCV, PFR, RSI, TV can complete at different rates
3. **Memory Safety**: BATCH_INSERT_SIZE controls memory usage
4. **Error Resilience**: Failed COPY batch doesn't block other symbols
5. **Async Compatible**: All methods support `asyncio.to_thread()` via async wrappers

## Performance Metrics (Observed)
- **1,181,001 records** inserted in **69.09 seconds** (~17,100 rows/sec)
- **10 active API symbols** with **3 proxies** in parallel
- **Zero "cannot affect row a second time"** errors post-migration

## Modified Files

### Core Files
1. **`back/dbsetup2.py`** - NEW: Combined legacy + COPY architecture with async logging
2. **`back/api_utils.py`** - UPDATED: Uses async execute_query directly
3. **`back/perp_input.py`** - UNCHANGED: Provides DB_RETENTION_DAYS, symbol lists

### API Scripts (Migrated)
1. **`apis/1ohlcv_pfr_h.py`** - UPDATED: Switched to new merge_data_to_stage()
2. **`apis/1ohlcv_pfr_ho.py`** - UPDATED: Same as above
3. **`apis/1z_rsi_tv_h.py`** - UPDATED: Similar pattern

### Remaining Migration
- **`calc_metrics.py`** - Should use `copy_to_metrics_stage()` + `merge_metrics_stage()`
- **Any other perp_data inserters** - Replace with `merge_data_to_stage()`

## Database Schema Notes

### perp_data Columns (All Numeric)
```
ts, symbol,
o, h, l, c, v, oi, pfr, lsr, tbv, tsv, rsi, lql, lqs
```

### perp_metrics Columns
Same as perp_data + 51 calculated percentage change columns
Used exclusively by `calc_metrics.py`

## Error Handling & Debugging

### Common Errors
1. **"invalid input syntax for type numeric: ""** → Missing null="\\N" in copy_from
2. **"ON CONFLICT DO UPDATE command cannot affect row a second time"** → Duplicate (ts, symbol) within same staging set
3. **Stalled script at "Database connection pool created"** → Missing `await` on async DBManager methods

### Logging Integration
- All methods support `api_utils.py` `log_status()` and `log_error()`
- `execute_query()` is now async for proper integration
- Status updates visible in console every 15 seconds

## Future Considerations

### 1. Column Versioning
To add new metrics:
1. Add nullable column to both `perp_data` and `perp_data_stage`
2. Update `merge_data_to_stage()` column list
3. Update all `process_data()` functions to populate new column

### 2. Backward Compatibility
- `insert_batch_data()` kept for emergency fallback
- All existing scripts should work with minor `import` changes

### 3. Performance Tuning Points
- `BATCH_INSERT_SIZE` (4000) - Balance between speed and memory
- `ThreadedConnectionPool` (3-25) - Adjust based on concurrent symbols
- `DB_RETENTION_DAYS` (20) - Data retention period

## Conclusion
The COPY staging architecture provides **10-50x speedup** for bulk inserts while maintaining full script compatibility. The unified `merge_data_to_stage()` method elegantly handles the multi-API reality of crypto data collection.

**Next Steps:**
1. Migrate `calc_metrics.py` to use perp_metrics staging
2. Add comprehensive unit tests for merge_data_to_stage()
3. Consider automatic column expansion via schema reflection