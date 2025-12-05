# back/api_utils.py. rev:4Dec 2025 ver:3;
import asyncio
import json
import logging
from datetime import datetime, timezone

from colorama import Fore, Style, init
from psycopg2 import sql

# Initialize colorama to auto-reset styles
init(autoreset=True)

# ============================================================================
#  LOGGING SETUP
# ============================================================================
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Standardized status types for frontend consistency
LOG_STATUS_TYPES = ["Started", "Running", "Completed", "Stopped", "Warning", "Error"]

LOG_COLORS = {
    "Started": Fore.CYAN,
    "Running": Fore.CYAN,
    "Completed": Fore.WHITE,
    "Stopped": Fore.YELLOW,
    "Warning": Fore.YELLOW,
    "Error": Fore.RED,
}

# ============================================================================
#  STANDARDIZED LOGGING UTILITIES
# ============================================================================

async def log_status(db_manager, script_name, status, message, details=None, proxies=None):
    """
    Asynchronously logs a status message to the console and the database.
    This function is non-blocking and safe to call from any async script.
    """
    if status not in LOG_STATUS_TYPES:
        logger.warning(f"'{status}' is not a standard status type. Consider using one of: {LOG_STATUS_TYPES}")

    color = LOG_COLORS.get(status, Fore.WHITE)
    
    if status == "Started":
        if 'days' in (details or {}):
            message += f" | {details['days']} days history"
        if proxies is not None:
            proxy_count = len(proxies)
            message += f" | {proxy_count} Proxies Active."
            if details is None: details = {}
            details['active_proxies'] = proxies

    logger.info(color + message)

    if not db_manager or not db_manager.conn:
        logger.warning("DBManager not available. Skipping database log for status.")
        return

    query = sql.SQL("INSERT INTO perp_status (ts, task_id, script_name, status, message, details) VALUES (DEFAULT, DEFAULT, %s, %s, %s, %s)")
    details_json = json.dumps(details) if details is not None else None
    
    try:
        await asyncio.to_thread(db_manager.execute_query, query, (script_name, status, message, details_json))
    except Exception as e:
        logger.error(Fore.RED + f"[DB LOG ERROR] Failed to write status to database: {e}")

async def log_error(db_manager, script_name, error_type, message, details=None):
    """
    Asynchronously logs an error message to the console and the database.
    This function is non-blocking and safe to call from any async script.
    """
    logger.error(Fore.RED + f"[{error_type}] {message}")

    if not db_manager or not db_manager.conn:
        logger.warning("DBManager not available. Skipping database log for error.")
        return

    query = sql.SQL("INSERT INTO perp_errors (ts, error_id, script_name, error_type, error_message, details) VALUES (DEFAULT, DEFAULT, %s, %s, %s, %s)")
    details_json = json.dumps(details) if details is not None else None
    
    try:
        await asyncio.to_thread(db_manager.execute_query, query, (script_name, error_type, message, details_json))
    except Exception as e:
        logger.error(Fore.RED + f"[DB LOG ERROR] Failed to write error to database: {e}")


# ============================================================================
#  TIMESTAMP NORMALIZATION UTILITY
# ============================================================================
def to_millis(ts):
    """
    Normalizes various timestamp formats to epoch milliseconds (UTC) as an integer.
    """
    if ts is None: return None
    if isinstance(ts, (int, float)):
        return int(ts * 1000) if ts < 1e12 else int(ts)
    if isinstance(ts, str):
        try:
            numeric_ts = float(ts)
            return int(numeric_ts * 1000) if numeric_ts < 1e12 else int(numeric_ts)
        except ValueError:
            try:
                dt_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return int(dt_obj.timestamp() * 1000)
            except ValueError:
                logger.error(f"Unsupported timestamp string format: {ts}")
                raise
    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


# ============================================================================
#  SYMBOL MAPPING UTILITY
# ============================================================================
def format_symbol(base_symbol, exchange):
    """
    Formats a base symbol into the exchange-specific perpetual format.
    """
    if exchange == 'binance':
        return f"{base_symbol}USDT"
    elif exchange == 'bybit':
        return f"{base_symbol}USDT"
    else:
        return base_symbol
