"""
back/proxy.py

A simple, production-ready module for managing a pool of proxies.
It provides a single function to get the next connection (either a proxy URL or local)
based on a configured ratio and round-robin rotation.

Designed to be used with curl-cffi's AsyncSession.
"""
import os
import random
import logging

# ============================================================================
#  PROXY CONFIGURATION - EDIT THIS LIST TO ENABLE/DISABLE PROXIES
# ============================================================================
# To disable a proxy, simply comment out its line in the list below.
_all_proxies = [
    os.getenv("PROXY_URL_1"),  # Main Webshare Proxy
    os.getenv("PROXY_URL_2"),  # Secondary IPRoyal - DISABLED due to connection errors
    os.getenv("PROXY_URL_3"),  # Tertiary Privado - DISABLED due to connection errors
]

# ============================================================================
#  BEHAVIOR SETTINGS
# ============================================================================
# The ratio of requests that should be sent through the proxy pool.
# 0.0 means never use proxies; 1.0 means always use proxies (if available).
USE_PROXY_RATIO = 0.7

# ============================================================================
#  INITIALIZATION (DO NOT EDIT)
# ============================================================================
CONFIGURED_PROXIES = [p for p in _all_proxies if p]
_proxy_index = 0

if not CONFIGURED_PROXIES:
    logging.warning("No active proxies are configured in proxy.py. Only the local connection will be used.")
else:
    logging.info(f"Initialized with {len(CONFIGURED_PROXIES)} active proxies.")

# ============================================================================
#  PUBLIC FUNCTION
# ============================================================================
def get_next_connection():
    """
    Determines the next connection to use (proxy or local) based on the ratio
    and rotates through the available proxies.

    Returns:
        tuple: (proxy_url, connection_identifier)
               - proxy_url (str or None): The full proxy URL for the request, or None for a local connection.
               - connection_identifier (str): A string for logging ('local' or the proxy URL).
    """
    global _proxy_index
    
    use_proxy_pool = CONFIGURED_PROXIES and random.random() < USE_PROXY_RATIO

    if use_proxy_pool:
        # Select the next proxy in the list using round-robin
        proxy_url = CONFIGURED_PROXIES[_proxy_index]
        _proxy_index = (_proxy_index + 1) % len(CONFIGURED_PROXIES)
        
        return proxy_url, proxy_url  # URL is both the proxy and its identifier
    else:
        # Use local connection
        return None, "local"
