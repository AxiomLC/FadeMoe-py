# master_api.py. rev:6Dec 2025 ver:1; 
# Master API orchestrator for Python scripts with proper execution flow
import asyncio
import signal
import os
import importlib.util
from datetime import datetime
import sys
from colorama import Fore, Style, init as colorama_init

colorama_init()

# Constants
BLOCK_1_PREFIX = "1"
BLOCK_2_PREFIX = "2"
TRIGGER_FILE = "1ohlcv_pfr_h.py"
TRIGGER_1Z_PREFIX = "1z-"
Z_DELAY_SECONDS = 10  # Delay for 1z scripts after trigger start
HEARTBEAT_INTERVAL = 60  # 60 seconds

# Console logging with purple color
STATUS_COLOR = Fore.LIGHTMAGENTA_EX + Style.BRIGHT
RESET = Style.RESET_ALL

# State tracking
running = False

def log_status(message):
    """Log status to console with purple color"""
    print(f"{STATUS_COLOR}{message}{RESET}")

def shutdown_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    print(f"\n{STATUS_COLOR}#### ⚠️ SHUTDOWN SIGNAL RECEIVED ####{RESET}")
    running = False
    asyncio.create_task(shutdown())

async def shutdown():
    """Gracefully shut down all running scripts"""
    print(f"{STATUS_COLOR}#### 🛑 STOPPING MASTER API ####{RESET}")
    
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"{STATUS_COLOR}#### ✅ MASTER API STOPPED ####{RESET}")

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
        else:
            # For scripts that just need to be imported with auto-execution
            # Just executing module is enough for scripts that run on import
            pass
            
        return True
    except Exception as e:
        print(f"❌ Error in {script_name}: {e}")
        return False

async def run_block1(scripts):
    """Run all Block 1 scripts in parallel"""
    block1_scripts = [s for s in scripts if s['block'] == 'block1']
    
    if not block1_scripts:
        log_status("#### ⚠️ NO BLOCK 1 SCRIPTS FOUND ####")
        return
    
    log_status(f"#### 🚦 STARTING BLOCK 1: {', '.join([s['name'] for s in block1_scripts])} ####")
    
    # Run all Block 1 scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block1_scripts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check if trigger file succeeded
    trigger_results = [r for r in results if isinstance(r, bool) and r]
    if len(trigger_results) > 0:
        log_status(f"#### ⚡ TRIGGER: {TRIGGER_FILE} COMPLETE - STARTING 1z SCRIPTS ####")
        # Start 1z scripts with delay
        await run_block1z(scripts)
    else:
        log_status(f"#### 🚨🚨🚨 CRITICAL: {TRIGGER_FILE} FAILED OR NOT FOUND - 1z SCRIPTS BLOCKED 🚨🚨🚨 ####")
    
    log_status("#### ✅ BLOCK 1 COMPLETE ####")

async def run_block1z(scripts):
    """Run all 1z scripts with delay after trigger"""
    block1z_scripts = [s for s in scripts if s['is_1z']]
    
    if not block1z_scripts:
        log_status("#### ⚠️ NO 1z SCRIPTS FOUND ####")
        return
    
    # Wait for delay before starting 1z scripts
    await asyncio.sleep(Z_DELAY_SECONDS)
    
    log_status(f"#### ⚡ STARTING 1z SCRIPTS: {', '.join([s['name'] for s in block1z_scripts])} ####")
    
    # Run all 1z scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block1z_scripts]
    await asyncio.gather(*tasks, return_exceptions=True)

async def run_block2(scripts):
    """Run all Block 2 scripts and start heartbeat"""
    block2_scripts = [s for s in scripts if s['block'] == 'block2']
    
    if not block2_scripts:
        log_status("#### ⚠️ NO BLOCK 2 SCRIPTS FOUND ####")
        return
    
    log_status(f"#### ⚡ STARTING BLOCK 2: {', '.join([s['name'] for s in block2_scripts])} ####")
    
    # Run all Block 2 scripts in parallel
    tasks = [run_script(script['path'], script['name']) for script in block2_scripts]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    log_status("#### 🎯 MASTER API FULLY OPERATIONAL! LIVE DATA ACTIVE ####")
    
    # Start heartbeat after Block 2 starts
    await start_heartbeat()

async def start_heartbeat():
    """Send periodic heartbeat messages"""
    global running
    running = True
    
    while running:
        try:
            log_status("#### ⏱️ master-api running real-time scripts ####")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            break

async def start_master(no_metrics=False):
    """Main master API startup"""
    log_status("#### 🚀 MASTER API INITIALIZING ####")
    
    # Discover scripts
    scripts = discover_scripts()
    log_status(f"#### 🚦 FOUND {len(scripts)} SCRIPTS IN apis/ ####")
    
    # Run in proper sequence
    await run_block1(scripts)
    await run_block2(scripts)
    
    # Conditionally start calc_metrics after all scripts are running but only if not in data-only mode
    if not no_metrics:
        try:
            # Import and run calc_metrics
            calc_metrics_spec = importlib.util.spec_from_file_location("calc_metrics", "back/calc_metrics.py")
            calc_metrics_module = importlib.util.module_from_spec(calc_metrics_spec)
            calc_metrics_spec.loader.exec_module(calc_metrics_module)
            
            if hasattr(calc_metrics_module, 'run_continuously') and callable(getattr(calc_metrics_module, 'run_continuously')):
                log_status("#### 📊 STARTING calc_metrics.py ####")
                asyncio.create_task(calc_metrics_module.run_continuously())
            else:
                log_status("#### ⚠️ calc_metrics.py does not have run_continuously function ####")
        except Exception as e:
            log_status(f"#### ❌ ERROR STARTING calc_metrics.py: {e} ####")

def main():
    """Main entry point"""
    global running
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Parse CLI arguments
    no_metrics = '-only' in sys.argv
    
    # Set up asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Start the master API
        loop.run_until_complete(start_master(no_metrics))
        
        # Keep the event loop running
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
    finally:
        loop.close()

if __name__ == "__main__":
    main()
