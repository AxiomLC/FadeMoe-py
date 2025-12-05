# Temporary README: Summary of Logging and DB Setup Decisions

**Date**: December 3, 2025

**Purpose**: This file summarizes the decisions made during a chat session regarding updates to shared logging and error catching functions in `api_utils.py`, subsequent edits to backfill ("h") scripts and the real-time ("c") script `2web_ohlc_lqtv_c.py`, and the resulting edit needed in `dbsetup.py` for the `perp_status` and `perp_errors` tables.

## Decisions and Planned Updates

### 1. Shared Logging and Error Catching in `api_utils.py`
- **Objective**: Enhance `api_utils.py` to include shared functions for the three-tiered logging system (`Started`, `Running`, `Stopped` for real-time; `Started`, `Running`, `Completed` for backfill) and a shared `log_heartbeat` function for periodic console updates.
- **Reason**: To ensure consistency in logging format, color schemes, and behavior across all scripts, reducing code duplication and standardizing output (e.g., light cyan for backfill, light green for real-time, yellow for stopped, bold white for completed. Colorama).
- **Planned Update**: Add functions like `start_script`, `update_running`, `complete_script`, `stop_script`, and `log_heartbeat` to `api_utils.py`. These will handle message formatting, database logging, and console output with script-type-specific parameters.

### 2. Updates to Backfill ("h") Scripts
- **Objective**: Adjust backfill scripts (e.g., `1ohlcv_pfr_h.py`, `1z_lq_h.py`) to use the new shared logging functions from `api_utils.py`.
- **Reason**: To reduce custom logging code in each script, ensuring uniform status messages and heartbeat updates (e.g., fetched and inserted counts in light cyan).
- **Planned Update**: Replace direct `log_status` calls with shared functions, update heartbeat to use `log_heartbeat` if implemented, and remove in script color handling as needed.

### 3. Major Update to Real-Time ("c") Script `2web_ohlc_lqtv_c.py`
- **Objective**: Update the real-time script to use shared logging functions from `api_utils.py`, finalize heartbeat format as `{SCRIPT_DEF}| OHLCV: X| LQ: Y| TV: Z` in light green, and ensure non-cumulative counts per heartbeat interval.
- **Reason**: To align with the shared logging system, provide consistent console output, and reflect activity since the last heartbeat (60-second interval) rather than cumulative totals.
- **Planned Update**: Implement shared logging calls, adjust heartbeat to reset counts after each display, and maintain light green color for real-time updates with yellow for stopped state.

### 4. Edit Needed in `dbsetup.py`
- **Objective**: Update the column order for `perp_status` and `perp_errors` tables to place `task_id`/`error_id` first, then `ts`, then `script_name`, followed by remaining fields, while maintaining `TIMESTAMP WITH TIME ZONE` for UTC alignment.
- **Reason**: To improve readability and logical organization of log tables, ensuring `ts` is positioned for efficient querying and display in UTC format (e.g., "2025-12-03 13:52:44 UTC") via client or query formatting, as requested for consistency with front-end display.
- **Specific Edit**: Reorder columns in `CREATE TABLE` statements for `perp_status` and `perp_errors` as follows:
  - `perp_status`: `task_id SERIAL PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(), script_name TEXT NOT NULL, status TEXT NOT NULL, message TEXT, details JSONB`
  - `perp_errors`: `error_id SERIAL PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(), script_name TEXT, error_type TEXT, error_message TEXT, details JSONB`
- **Additional Consideration**: Add retention policies and indexes on `ts DESC` and `script_name` for log tables to manage data growth and improve query performance for descending timestamp order.

## Next Steps
- Update `api_utils.py` with shared logging functions as outlined.
- Apply small updates to backfill scripts to use these shared functions.
- Perform a major update to `2web_ohlc_lqtv_c.py` for logging and heartbeat formatting.
- Apply the column reordering edit to `dbsetup.py` for `perp_status` and `perp_errors`, along with retention and indexing enhancements.

**Note**: This temporary README serves as a record of decisions made during the chat session due to context limitations and repeated tool errors. It should be reviewed and integrated into permanent documentation or README files as needed.
