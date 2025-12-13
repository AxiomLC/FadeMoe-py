# iproyaltest.py - Test IPRoyal proxies against Binance API
import asyncio
import time
from datetime import datetime, timezone
from curl_cffi.requests import AsyncSession
from colorama import Fore, Style, init

init(autoreset=True)

# ============================================================================
# CONFIGURATION
# ============================================================================
IPROYAL_CONFIG = {
    "host": "206.53.49.228",
    "user": "14a233d28dd8f",
    "password": "bf64d81ae2",
    "socks5_ports": [10324, 11324, 13324, 14324, 15324, 22326, 22324, 22325, 12324],
    "http_ports": [10323, 11323, 12323, 12325, 12326, 13323, 14323, 15323, 22323]
}

# Binance test endpoints (simple, fast responses)
TEST_ENDPOINTS = [
    "https://fapi.binance.com/fapi/v1/ping",  # Simple ping
    "https://fapi.binance.com/fapi/v1/time",  # Server time
    "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol=BTCUSDT",  # Symbol info
]

TIMEOUT = 10  # seconds
TEST_ITERATIONS = 3  # Test each proxy 3 times for consistency

# ============================================================================
# TEST LOGIC
# ============================================================================
async def test_proxy(session, proxy_url, proxy_name, endpoint):
    """Test a single proxy against a single endpoint. Returns (success, latency_ms, error)"""
    try:
        start = time.time()
        response = await session.get(
            endpoint, 
            proxies={"https": proxy_url, "http": proxy_url},
            timeout=TIMEOUT
        )
        latency_ms = (time.time() - start) * 1000
        
        if response.status_code == 200:
            return True, latency_ms, None
        else:
            try:
                error_data = response.json()
                error_msg = f"{response.status_code} - {error_data.get('msg', 'Unknown')}"
            except:
                error_msg = f"{response.status_code}"
            return False, latency_ms, error_msg
            
    except asyncio.TimeoutError:
        return False, TIMEOUT * 1000, "Timeout"
    except Exception as e:
        error_msg = str(e)[:100]  # Truncate long errors
        return False, 0, error_msg

async def test_proxy_comprehensive(session, proxy_url, proxy_name):
    """Run comprehensive test on a proxy: multiple endpoints, multiple iterations"""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.CYAN}Testing: {proxy_name}")
    print(f"{Fore.CYAN}{'='*80}")
    
    results = {
        "name": proxy_name,
        "url": proxy_url,
        "successes": 0,
        "failures": 0,
        "total_latency_ms": 0,
        "min_latency_ms": float('inf'),
        "max_latency_ms": 0,
        "errors": []
    }
    
    total_tests = len(TEST_ENDPOINTS) * TEST_ITERATIONS
    
    for iteration in range(TEST_ITERATIONS):
        for endpoint_idx, endpoint in enumerate(TEST_ENDPOINTS):
            endpoint_name = endpoint.split('/')[-1] or "ping"
            
            success, latency_ms, error = await test_proxy(session, proxy_url, proxy_name, endpoint)
            
            if success:
                results["successes"] += 1
                results["total_latency_ms"] += latency_ms
                results["min_latency_ms"] = min(results["min_latency_ms"], latency_ms)
                results["max_latency_ms"] = max(results["max_latency_ms"], latency_ms)
                print(f"  {Fore.GREEN}✓ {endpoint_name} [{iteration+1}/{TEST_ITERATIONS}]: {latency_ms:.0f}ms")
            else:
                results["failures"] += 1
                results["errors"].append(error)
                print(f"  {Fore.RED}✗ {endpoint_name} [{iteration+1}/{TEST_ITERATIONS}]: {error}")
            
            await asyncio.sleep(0.2)  # Small delay between requests
    
    # Calculate stats
    if results["successes"] > 0:
        results["avg_latency_ms"] = results["total_latency_ms"] / results["successes"]
    else:
        results["avg_latency_ms"] = 0
        results["min_latency_ms"] = 0
    
    results["success_rate"] = (results["successes"] / total_tests) * 100
    
    return results

async def run_tests():
    """Run all proxy tests"""
    print(f"\n{Fore.YELLOW}{Style.BRIGHT}{'='*80}")
    print(f"{Fore.YELLOW}{Style.BRIGHT}IPRoyal Proxy Test Suite")
    print(f"{Fore.YELLOW}{Style.BRIGHT}Testing {len(IPROYAL_CONFIG['socks5_ports']) + len(IPROYAL_CONFIG['http_ports'])} proxies")
    print(f"{Fore.YELLOW}{Style.BRIGHT}{'='*80}\n")
    
    all_results = []
    
    async with AsyncSession() as session:
        # Test SOCKS5 proxies
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}═══ TESTING SOCKS5 PROXIES ═══")
        for port in IPROYAL_CONFIG["socks5_ports"]:
            proxy_url = f"socks5://{IPROYAL_CONFIG['user']}:{IPROYAL_CONFIG['password']}@{IPROYAL_CONFIG['host']}:{port}"
            proxy_name = f"SOCKS5:{port}"
            results = await test_proxy_comprehensive(session, proxy_url, proxy_name)
            all_results.append(results)
            await asyncio.sleep(0.5)
        
        # Test HTTP proxies
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}═══ TESTING HTTP PROXIES ═══")
        for port in IPROYAL_CONFIG["http_ports"]:
            proxy_url = f"http://{IPROYAL_CONFIG['user']}:{IPROYAL_CONFIG['password']}@{IPROYAL_CONFIG['host']}:{port}"
            proxy_name = f"HTTP:{port}"
            results = await test_proxy_comprehensive(session, proxy_url, proxy_name)
            all_results.append(results)
            await asyncio.sleep(0.5)
    
    # Print summary
    print(f"\n\n{Fore.YELLOW}{Style.BRIGHT}{'='*80}")
    print(f"{Fore.YELLOW}{Style.BRIGHT}SUMMARY REPORT")
    print(f"{Fore.YELLOW}{Style.BRIGHT}{'='*80}\n")
    
    # Sort by success rate, then by avg latency
    all_results.sort(key=lambda x: (-x["success_rate"], x["avg_latency_ms"]))
    
    print(f"{'Proxy':<15} {'Success Rate':<15} {'Avg Latency':<15} {'Min':<10} {'Max':<10} {'Status'}")
    print(f"{'-'*80}")
    
    working_proxies = []
    failed_proxies = []
    
    for r in all_results:
        if r["success_rate"] == 100:
            color = Fore.GREEN
            status = "✓ WORKING"
            working_proxies.append(r)
        elif r["success_rate"] > 0:
            color = Fore.YELLOW
            status = "⚠ PARTIAL"
        else:
            color = Fore.RED
            status = "✗ FAILED"
            failed_proxies.append(r)
        
        avg = f"{r['avg_latency_ms']:.0f}ms" if r['avg_latency_ms'] > 0 else "N/A"
        min_lat = f"{r['min_latency_ms']:.0f}ms" if r['min_latency_ms'] != float('inf') else "N/A"
        max_lat = f"{r['max_latency_ms']:.0f}ms" if r['max_latency_ms'] > 0 else "N/A"
        
        print(f"{color}{r['name']:<15} {r['success_rate']:.0f}%{'':<11} {avg:<15} {min_lat:<10} {max_lat:<10} {status}")
    
    # Recommendations
    print(f"\n{Fore.CYAN}{Style.BRIGHT}RECOMMENDATIONS:")
    if working_proxies:
        print(f"{Fore.GREEN}✓ {len(working_proxies)} fully working proxies found")
        fastest = working_proxies[0]
        print(f"{Fore.GREEN}  → Fastest: {fastest['name']} ({fastest['avg_latency_ms']:.0f}ms avg)")
        
        print(f"\n{Fore.CYAN}Top 3 proxies to use:")
        for i, proxy in enumerate(working_proxies[:3], 1):
            print(f"  {i}. {proxy['name']}: {proxy['avg_latency_ms']:.0f}ms avg")
    
    if failed_proxies:
        print(f"\n{Fore.RED}✗ {len(failed_proxies)} failed proxies:")
        for proxy in failed_proxies:
            print(f"  → {proxy['name']}: {proxy['errors'][0] if proxy['errors'] else 'Unknown error'}")

if __name__ == "__main__":
    asyncio.run(run_tests())