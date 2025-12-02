27 Nov update:
To run the fademoe5 app inside docker. postrgres db connection  to localhost:5433 to not conflict with local .js app accessing port 5432. Entire application (Python code + TimescaleDB database) runs inside Docker containers orchestrated by docker-compose.yml.

Docker app indicates: inside "fademoe5" container:
db:
postgres:latest
5433:5432⁠
image ID: sha256:5ec39c188013123927f30a006987c6b0e20f3ef2b54b140dfa96dac6844d883f
volume: fademoe5_db_data

app:
fademoe5-app
8000:8000
image ID: sha256:de53b0c3113d1f69ecfa4f3b8ccd62696f040f5294057dac1eaec4ad74b266dc
--------------------------------------------------------------

2025-11-25 - Version 1
1) Crypto perps API data fetching app for Binance perp params, flexible to expand to additional APIs with data points re: perpetual futures. Runs off a 20 day (default) rolling self pruning timescale hypertable postgres db. App and db in Docker container.
2) This python app will be based off successful javascript app: fademoe4, which fetched from Binance, Bybit and Okx. This app will streamline and relegate to Binance only to start.
3) User dev controls at top of perp-input.py should allow alteration of size of db from 10-60 days. 

Here's the refined application structure and flow with more detailed documentation:

Application Structure
Detailed Function Flow with README
Key Script Documentation
- main.py - Application entry point
- perp-input.py - User input for crypto symbols, and other global settings.
- back/dbsetup.py - Database operations
- back/calc-metrics.py - take raw params from perp_data table and calc chg_1m, chg_5m, and chg_10 params for each and insert to perp_metrics table (chg_ params for: ohlcv, oi, pfr, lsr, rsi, tbv, tsv, lql, lqs)
- back/brute.py - Backtesting engine
- back/tune.py - Backtesting fine tune engine
- apis/realtime.py, backfill.py & others - fill perp_data table from crypto perpetual futures data from Binance: ohlcv, oi, pfr, lsr, rsi, tbv, tsv, lql, lqs

- front/ css, main, index and view\ html files for 3 difft div views. Along with their control files: control buttons and functions. 

Tree:

fademoe5/
├── .env
├── main.py
├── master-api.py
├── perp-input.py
├── requirements.txt
├── back/
│   ├── dbsetup.py
│   ├── calc-metrics.py
│   ├── brute.py
│   ├── tune.py
├── apis/
│   ├── realtime.py
│   └── backfill.py
└── front/
    ├── app.py
    ├── styles.css
    ├── index.html
    ├── views/
        ├── dbview.html
        ├── cardsview.html
        └── backtester.html
    └── control/
        ├── dbcontrol.py
        ├── cardcontrol.py
        ├── btcontrol.py
        └── statusbox.py
        
        """
Main application controller.
Handles:
- Initialization of all components
- Coordination between backfill, real-time, and backtester
- Future UI integration points
"""

"""
Database manager for TimescaleDB.
Features:
- Connection pooling for performance
- Optimized queries for time-series data
- Chunked inserts for large datasets
- Retention policy enforcement (20-day rolling)
- Error handling and logging
"""
"""
Backtesting engines with user-configurable parameters.
Key functions:
- parse_algo_string: Parse algorithm strings
- expand_parsed_to_atomic: Expand algorithm combinations
- apply_cascading_binary: Apply cascading logic
- simulate_trades: Simulate trades with TP/SL
- calculate_metrics: Calculate performance metrics

Configuration:
- Trade settings (minPF, tradeDir, etc.)
- Algorithm parameters
- Output preferences, console display and dynamic json files in brute\ or tune\ sub-folders. 
- Speed optimizations
"""
# User-configurable parameters at top of tune.py
CONFIG = {
    "TradeSettings": {
        "minPF": 0.3,
        "tradeDir": "Long",  # 'Long' | 'Short' | 'Both'
        "tradeSymbol": {"useAll": True, "list": ["ETH", "BTC"]},
        "trade": {
            "tradeWindow": 20,  # minutes
            "posVal": 1000,     # position value
            "tpPerc": [0.7, 1.2, 1.5],  # take profit percentages
            "slPerc": [0.3, 0.4, 0.7]   # stop loss percentages
        },
        "minTrades": 20,
        "maxTrades": 1500
    },

    "ComboAlgos": [
        "MT; bin; rsi1_chg_5m; >; [20,40,60]",
        "All; bin; [params]; >; [corePerc]"
    ],

    "AlgoSettings": {
        "algoWindowMinutes": 60,
        "algoSymbol": {"useAll": False, "list": ["ETH", "BTC"]},
        "corePerc": [0.2, 0.5, 1.2, 5, 35, 100],
        "params": [
            "v_chg_1m", "v_chg_5m", "v_chg_10m",
            "oi_chg_1m", "oi_chg_5m", "oi_chg_10m",
            "pfr_chg_1m", "pfr_chg_5m", "pfr_chg_10m",
            "lsr_chg_1m", "lsr_chg_5m", "lsr_chg_10m",
            "rsi1_chg_1m", "rsi1_chg_5m", "rsi1_chg_10m",
            "rsi60_chg_1m", "rsi60_chg_5m", "rsi60_chg_10m",
            "tbv_chg_1m", "tbv_chg_5m", "tbv_chg_10m",
            "tsv_chg_1m", "tsv_chg_5m", "tsv_chg_10m",
            "lql_chg_1m", "lql_chg_5m", "lql_chg_10m",
            "lqs_chg_1m", "lqs_chg_5m", "lqs_chg_10m"
        ]
    },

    "Output": {
        "topAlgos": 15,
        "listAlgos": 30,
        "outputTradeTS": False,
        "devMode": True  # Write to JSON file
    },

    "Speed": {
        "fetchParallel": 8,
        "simulateParallel": 8,
        "chunkMinutes": 720
    }
}
"""
Backfilling scripts via Rest API fetch from Binance, and Coinalyz, or others.
Process:
1. Determine missing data periods
2. Fetch data from Binance API
3. Process and insert into database
4. Prune old data (keep 20 days)

Configuration:
- Symbols to backfill
- Time range
- Data types to fetch
"""
Real-time data processor using Binance WebSocket.
Features:
- Connection management
- Data validation
- Trigger detection
- Database updates
- Error handling and logging
"""

Clear separation of concerns
User-configurable parameters
Future UI integration points
Detailed documentation
Maintainable code structure
The backtester brute and tune are designed to be flexible enough for both script-based and future UI-based parameter input, while maintaining all the core functionality of your JavaScript version.

"""
SQLAlchemy & CeleryHow: to Use Them Later (Without Breaking Old Code)
- **SQLAlchemy**: Start small—import it only in a new function (e.g., in `control/dbcontrol.py`): `from sqlalchemy import create_engine`. Use it alongside `psycopg2` (e.g., for fancy queries), but keep `dbsetup.py` untouched until you're comfy.
- **Celery**: Set up a basic "app" in `main.py` (e.g., `from celery import Celery`), then define tasks like `@celery.task` in `back/brute.py` for backtests. Old scripts run as-is.
- Test Gradually: Run `python -c "import sqlalchemy; import celery; print('All good!')"` to verify. If issues (rare), `pip uninstall` them.

"""
why Docker?
- **Why Docker?** It bundles everything (code, DB, ports) into reusable "containers" (like apps in a sandbox). Your local Python tests are for dev; Docker is for real use (avoids conflicts, like the port 5432 local vs. 5433 in Docker).
- **Key Parts from docker-compose.yml**:
  - **db Container** (TimescaleDB): The database "box." Runs Postgres with time-series magic. Internal port 5432, exposed to your computer at 5433 (so tools like pgAdmin can connect locally without entering the box). Uses volume `fademoe5_db_data` to save data even if you stop it.
  - **app Container** (FadeMoe5 Python): Runs `main.py` (FastAPI server on port 8000, exposed to your computer at 8000). Depends on DB being healthy first. Volumes mount your code (./:/app) for live edits.
  - **Networks/Volumes**: Internal network (`fademoe-network`) lets app talk to DB via 'db' name (no localhost). Volumes persist DB data.
- **How to Run** (Step-by-Step):
  1. Install Docker Desktop (free, from docker.com—Windows/Mac/Linux).
  2. Open terminal in `FadeMoe5/`, run `docker-compose up --build` (builds app image, starts both containers—takes 1-5 min first time).
  3. Check: `docker ps` (shows running containers, ports like 8000->8000/tcp for app).
  4. Test: Browser to `http://localhost:8000/` (Hello World) or `/health` (DB check—should be "healthy").
  5. Stop: Ctrl+C or `docker-compose down` (stops, saves data). `docker-compose up -d` for background run.
- **Troubleshooting**:
  - No ports? Run `docker-compose down -v` (cleans volumes if stuck), then up again.
  - DB not healthy? Check logs: `docker logs fademoe5-db-1` (password/auth issues common).
  - Edit code? `docker-compose up --build` reloads.

To expose app views (dbview.html, cardsview.html, or full index.html dashboard) to external sites like your WordPress fade.moe, use iframe embedding—the app serves them as HTML routes on the VPS (Docker on port 8000). CORS in main.py allows cross-site loads without blocks. On VPS, ensure `docker-compose up -d` and `ufw allow 8000` (firewall open). Add routes in main.py if needed (e.g., `@app.get('/db') return HTMLResponse(dbview_html)`; similar for /cards and /index). Then, in WordPress admin (Custom HTML block):
<iframe src="http://your-vps-ip:8000/cards" width="100%" height="500" frameborder="0" style="border:none;"></iframe>

Replace src with IP:8000/route (e.g., /db for table view, / for full dashboard with embeds). For clean URL, add domain (app.fade.moe → VPS IP in DNS), use Nginx proxy to port 80/443 (config: location / { proxy_pass http://localhost:8000; }), and HTTPS via Certbot. Test: Browser loads iframe from any site—external frontends fetch via fetch('/api/data') or embed seamlessly. Update routes for real views (load from front/views/).
