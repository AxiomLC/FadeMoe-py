# master_api.py. rev:6Dec 2025 ver:12; Simplified to only orchestrate script startup, no shutdown logic
import asyncio
import signal
import os
import importlib.util
import sys
from colorama import Fore, Style, init as colorama_init

colorama_init()

# Constants
BLOCK_1_PREFIX = "1"
BLOCK_2_PREFIX = "2"
TRIGGER_FILE = "1ohlcv_pfr_h.py"
TRIGGER_1Z_PREFIX = "1z_"
Z_DELAY_SECONDS = 10  # Delay for 1z scripts after trigger start
HEARTBEAT_INTERVAL = 60  # 60 seconds

# Console logging with purple color
STATUS_COLOR = Fore.LIGHTMAGENTA_EX + Style.BRIGHT
RESET = Style.RESET_ALL

def log_status(message):
    """Log status to console with purple color"""
    print(f"{STATUS_COLOR}{message}{RESET}")

def log_error(message):
    """Log error to console with purple color"""
    print(f"{STATUS_COLOR}{message}{RESET}")

async def run_script(script_path, script_name):
    """Execute a Python script module"""
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location("module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Execute the script's main function based on convention
        if hasattr(module, 'execute') and callable(getattr(module, 'execute')):
            # For scripts that have execute function (like 2oi_lsr_c.py)
            await module.execute()
        elif hasattr(module, 'run') and callable(getattr(module, 'run')):
            # For scripts that have run function (legacy)
            await module.run()
        elif hasattr(module, 'main') and callable(getattr(module, 'main')):
            # For scripts that have main function (like 1ohlcv_pfr_h.py)
            await module.main()
        else:
            # For scripts that just need to be imported with auto-execution
            # This handles scripts with __main__ entry or just that execute on import
            pass
            
        return True
    except Exception as e:
        log_error(f"❌ Error in {script_name}: {e}")
        return False

async def run_block1(scripts):
    """Run all Block 1 scripts in parallel"""
    block1_scripts = [s for s in scripts if s['block'] == 'block1']
    
    if not block1_scripts:
        log_status("#### ⚠️ NO BLOCK 1 SCRIPTS FOUND ####")
        return
    
    log_status(f"🚦STARTING BLOCK 1: {', '.join([s['name'] for s in block1_scripts])}")
    
    # Run all Block 1 scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block1_scripts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check if trigger file succeeded
    trigger_results = [r for r in results if isinstance(r, bool) and r]
    
    if len(trigger_results) > 0:
        # Trigger file completed successfully - immediately start 1z scripts with delay
        log_status("TRIGGER: 1ohlcv_pfr_h.py COMPLETE - STARTING 1z SCRIPTS")
        # Start 1z scripts with delay from when trigger file starts
        await run_block1z(scripts)
    else:
        log_status("#### 🚨🚨🚨 CRITICAL: 1ohlcv_pfr_h.py FAILED OR NOT FOUND - 1z SCRIPTS BLOCKED 🚨🚨🚨 ####")
    
    log_status("✔️ BLOCK 1 COMPLETE")

async def run_block1z(scripts):
    """Run all 1z scripts with delay after trigger start"""
    block1z_scripts = [s for s in scripts if s['is_1z']]
    
    if not block1z_scripts:
        log_status("#### ⚠️ NO 1z SCRIPTS FOUND ####")
        return
    
    # Wait for delay before starting 1z scripts
    await asyncio.sleep(Z_DELAY_SECONDS)
    
    log_status(f"🚥 STARTING 1z SCRIPTS: {', '.join([s['name'] for s in block1z_scripts])}")
    
    # Run all 1z scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block1z_scripts]
    await asyncio.gather(*tasks, return_exceptions=True)

async def run_block2(scripts):
    """Run all Block 2 scripts and start heartbeat"""
    block2_scripts = [s for s in scripts if s['block'] == 'block2']
    
    if not block2_scripts:
        log_status("#### ⚠️ NO BLOCK 2 SCRIPTS FOUND ####")
        return
    
    log_status("✔️ Trigger File complete. Starting Block 2 & Calc_Metrics.")
    
    # Run all Block 2 scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block2_scripts]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    log_status("MASTER API FULLY OPERATIONAL. LIVE DATA ACTIVE")
    
    # Start heartbeat after Block 2 starts
    await start_heartbeat()

async def start_heartbeat():
    """Send periodic heartbeat messages"""
    while True:
        try:
            log_status("🚥 master-api running real-time scripts")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            break

async def start_master(no_metrics=False):
    """Main master API startup with exact 5 log sections"""
    # #1 "🚀 MASTER API INITIALIZING"
    log_status("🚀 MASTER API INITIALIZING")
    
    # Discover scripts
    scripts = discover_scripts()
    
    # Error check for scripts in apis/
    if len(scripts) == 0:
        log_error("❌ ERROR: No scripts found in apis/ folder")
        raise Exception("No scripts found in apis/ folder")
    
    # #2 "Master_Api found {#} scripts in apis\\. Trigger file is {TRIGGER_FILE}. Staring Block 1."
    # This should be exactly one line with your format - NO EMOJIS or ### symbols
    log_status(f"Master_Api found {len(scripts)} scripts in apis\\. Trigger file is {TRIGGER_FILE}. Staring Block 1.")
    
    # Run in proper sequence
    await run_block1(scripts)
    
    # Error check for calc_metrics in back/
    try:
        calc_metrics_spec = importlib.util.spec_from_file_location("calc_metrics", "back/calc-metrics.py")
        calc_metrics_module = importlib.util.module_from_spec(calc_metrics_spec)
        calc_metrics_spec.loader.exec_module(calc_metrics_module)
    except Exception as e:
        log_error(f"❌ ERROR: Failed to load calc_metrics.py: {e}")
        raise Exception("Failed to load calc_metrics.py")
    
    await run_block2(scripts)
    
    # #4 "Master_Api running real-time" (heartbeat)
    log_status("Master_Api running real-time")

def discover_scripts():
    """Discover Python scripts in apis/ folder"""
    scripts = []
    apis_dir = "apis"
    
    if not os.path.exists(apis_dir):
        return scripts
    
    for filename in os.listdir(apis_dir):
        if filename.endswith('.py') and not filename.startswith('__'):
            filepath = os.path.join(apis_dir, filename)
            scripts.append({
                'name': filename,
                'path': filepath,
                'block': determine_block(filename),
                'is_trigger': filename == TRIGGER_FILE,
                'is_1z': filename.startswith(TRIGGER_1Z_PREFIX)
            })
    
    return scripts

def determine_block(filename):
    """Determine script block based on filename prefix"""
    if filename.startswith(BLOCK_1_PREFIX) and not filename.startswith(TRIGGER_1Z_PREFIX):
        return 'block1'
    elif filename.startswith(TRIGGER_1Z_PREFIX):
        return '1z'
    elif filename.startswith(BLOCK_2_PREFIX):
        return 'block2'
    return 'unknown'

def main():
    """Main entry point - only start the scripts, let system handle shutdown"""
    # Parse CLI arguments
    no_metrics = '-only' in sys.argv
    
    # Set up asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Start the master API - which will start all scripts in proper sequence
        loop.run_until_complete(start_master(no_metrics))
        
        # Keep the event loop running (the scripts run in parallel)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            # User pressed Ctrl+C - let each script handle its own shutdown
            print(f"\n{STATUS_COLOR}Master_Api smoothly stopped.{RESET}")
            pass
    finally:
        loop.close()

if __name__ == "__main__":
    main()
