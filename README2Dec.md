# FadeMoe-py

FadeMoe-py is a high-performance, asynchronous data pipeline and backtesting engine for cryptocurrency perpetual futures. This project is a complete Python port and architectural enhancement of the original Node.js version, focusing exclusively on the Binance ecosystem. It is designed for speed, reliability, and scalability, leveraging a modern Dockerized environment with a TimescaleDB backend.

The system's core function is to perform massive historical backfills of various market data types (OHLCV, Funding Rates, Open Interest, Liquidations, etc.), calculate derivative metrics (RSI, Taker Volume), and store them in a structured time-series database. This data then serves as the foundation for complex algorithmic backtesting and, in the future, real-time trading analysis.

---

## Architecture & Core Technologies

The application is fully containerized using Docker and Docker Compose, ensuring a consistent and reproducible environment for both development and production.

*   **Application (`app` service):** A Python container running the suite of data-fetching scripts.
    *   **Concurrency:** Leverages `asyncio` and `curl-cffi` for high-speed, parallel fetching of API data.
    *   **Resilience:** Integrates a proxy pool via `back/proxy.py` to manage rate limits and distribute network load.
    *   **Performance:** All blocking database operations are run in separate threads via `asyncio.to_thread` to prevent the main event loop from stalling, ensuring maximum throughput.

*   **Database (`db` service):** A PostgreSQL container with the **TimescaleDB** extension enabled for superior time-series data performance.
    *   **Tuning:** The environment uses a custom `postgresql.conf` file, tuned for high-volume ingestion and fast queries. This file is mounted directly into the container and should be adjusted based on the host machine's resources.
    *   **Schema:** A single `perp_data` hypertable serves as the central repository for all time-series data, indexed by `(ts, symbol)` for efficient upserts.

---

## Project Structure

```
FadeMoe5/
│
├── .env                  # Environment variables (DB credentials, API keys)
├── docker-compose.yml    # Defines and configures the app and db services
├── Dockerfile            # Instructions to build the Python 'app' container
├── requirements.txt      # Python package dependencies
├── postgresql.conf       # Tuned configuration for the PostgreSQL database
├── README.md             # This file
│
├── apis/                 # Contains all historical data backfill scripts
│   ├── READMEapis.md     # Detailed documentation for the API scripts
│   ├── 1ohlcv_pfr_h2.py  # Foundational OHLCV and PFR backfill
│   ├── 1oi_lsr_h.py      # Open Interest and Long/Short Ratio backfill
│   ├── 1z_lq_h.py        # Liquidations backfill (from Coinalyze)
│   └── 1z_rsi_tv-h.py    # RSI calculation and Taker Volume backfill
│
└── back/                 # Core backend modules and shared utilities
    ├── api_utils.py      # Shared async logging functions (log_status, log_error)
    ├── dbsetup.py        # DBManager class and initial table setup logic
    ├── proxy.py          # Proxy pool management
    └── perp_input.py     # Central list of symbols and core constants
```

---

## Getting Started

### Prerequisites
*   Docker Desktop installed and running.
*   Git installed.
*   A `.env` file in the project root with your database credentials and `COINALYZE_KEY`.

### Initial Setup

1.  **Clone the repository:**
    ```shell
    git clone https://github.com/AxiomLC/FadeMoe-py.git
    cd FadeMoe-py
    ```
2.  **Build and Start the Docker Containers:**
    This will build the Python image, install dependencies from `requirements.txt`, create the database, and start the services in the background.
    ```shell
    docker-compose up -d --build
    ```
3.  **Set up the Database Schema:**
    The first time you run the project, you need to create the database tables. Execute the `dbsetup.py` script inside the running `app` container.
    ```shell
    docker-compose exec app python back/dbsetup.py
    ```

### Running the Backfill Scripts

The backfill scripts should be run in a specific order to ensure data dependencies are met.

1.  **Run Foundational Data Scripts:** Start with the scripts that provide the core data.
    ```shell
    # Fetches OHLCV and Premium Funding Rate
    docker-compose exec app python apis/1ohlcv_pfr_h2.py
    # Fetches Open Interest and Long/Short Ratio
    docker-compose exec app python apis/1oi_lsr_h.py
    ```
2.  **Run Enrichment Scripts:** Run the scripts that calculate new metrics based on the foundational data.
    ```shell
    # Fetches Liquidations from Coinalyze
    docker-compose exec app python apis/1z_lq_h.py
    # Calculates RSI and Taker Volume
    docker-compose exec app python apis/1z_rsi_tv-h.py
    ```

---

## Future Development

*   **Backtesting Engine:** The next major phase is to port the backtesting engine from the original JavaScript version, which will run complex algorithmic tests against the data populated by these scripts.
*   **Real-time Data:** A suite of `2...` series scripts will be developed to handle continuous, real-time data ingestion, likely using WebSockets.
*   **Master Orchestrator:** A `master-api.py` script will be created to manage and schedule the execution of all backfill scripts in the correct order.
