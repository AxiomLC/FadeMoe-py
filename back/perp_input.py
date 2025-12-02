# perp-input.py
# This file contains user-configurable settings for the application.
# In the future, these static values could be replaced by inputs from a UI.

# ============================================================================
# DATABASE SETTINGS
# ============================================================================

# The number of days to retain data in the database hypertables.
DB_RETENTION_DAYS = 20

# ============================================================================
# SYMBOL LIST
# ============================================================================

# Static list of perpetual trading symbols to be used by the application.
# Note: Duplicate symbols like 'SUI' and 'SPX' are included as provided.
BASE_SYMBOLS = [
    'ETH',
    'XRP',
    'HYPE',
    'BTC',
    'SOL',
    'DOGE',
    'JUP',
    'LINEA',
    'LINK',
    'SPX',
    'STRK',
    'SUI',
    'AVAX',
    'ENA',
    'SUI',
    'SPX',
    'ASTER',
    'HYPE',
    'IP',
    'NEAR',
]
