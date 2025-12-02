# back/dbsetup.py
# ============================================================================
# DATABASE SETUP & MANAGER
# This script is designed to be both a standalone setup utility and an
# importable module for database operations.
#
# To run as a standalone script for fresh setup (drops existing tables):
# python back/dbsetup.py
# ============================================================================
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from psycopg2 import sql

# Import user-configurable settings from perp_input.py
try:
    from back.perp_input import DB_RETENTION_DAYS
except ImportError:
    print(
        "Warning: perp_input.py not found. Using default retention period of 20 days."
    )
    DB_RETENTION_DAYS = 20

# Load environment variables from .env file
load_dotenv()


class DBManager:
    def __init__(self):
        """Initializes the database manager and connection pool."""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                dbname=os.getenv("DB_NAME"),
            )
            print("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            print(f"❌ Could not connect to the database: {e}")
            self.conn = None

    def execute_query(self, query, params=None, fetch=None):
        """
        Executes a SQL query.

        Args:
            query (str or sql.SQL): The SQL query to execute.
            params (tuple, optional): Parameters to pass to the query.
            fetch (str, optional): Type of fetch ('one', 'all').

        Returns:
            Result of the fetch operation or None.
        """
        if not self.conn:
            print("❌ No database connection.")
            return None

        with self.conn.cursor() as cur:
            try:
                cur.execute(query, params)
                self.conn.commit()
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
            except psycopg2.Error as e:
                print(f"❌ Query failed: {e}")
                self.conn.rollback()
                return None

    def close_connection(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed.")

    def insert_batch_data(self, data):
        """
        Inserts a batch of data and returns the number of rows affected.
        """
        if not data:
            return 0

        if not self.conn:
            print("❌ No database connection.")
            return 0

        cols = list(data[0].keys())
        cols_sql = sql.SQL(",").join(map(sql.Identifier, cols))
        update_cols_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ['ts', 'symbol']])
        
        query = sql.SQL("""
            INSERT INTO perp_data ({})
            VALUES %s
            ON CONFLICT (ts, symbol) DO UPDATE SET {}
        """).format(cols_sql, sql.SQL(update_cols_str))
        
        data_tuples = [tuple(d.get(col) for col in cols) for d in data]

        with self.conn.cursor() as cur:
            try:
                psycopg2.extras.execute_values(cur, query, data_tuples)
                self.conn.commit()
                return cur.rowcount
            except psycopg2.Error as e:
                print(f"❌ Batch insert failed: {e}")
                self.conn.rollback()
                return 0

    def setup_database(self):
        """
        Main function to set up the database from scratch.
        It drops existing tables, enables extensions, creates new tables,
        and sets up hypertables with retention policies.
        """
        print("⚙️ Setting up database from scratch...")
        self._drop_existing_tables()
        self._enable_extensions()
        self._create_core_tables()
        self._setup_hypertables_and_policies()
        print("✅ Database setup complete.")

    def _drop_existing_tables(self):
        """Drops the core application tables if they exist."""
        print("  - Dropping existing tables...")
        tables = [
            "perp_data",
            "perp_metrics",
            "perp_status",
            "perp_errors",
            "combo_algos",  # Placeholder for future
        ]
        for table in tables:
            query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                sql.Identifier(table)
            )
            self.execute_query(query)
            print(f"    - Dropped table: {table}")

    def _enable_extensions(self):
        """Enables the TimescaleDB extension."""
        print("  - Enabling extensions...")
        query = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"
        self.execute_query(query)
        print("    - TimescaleDB extension enabled.")

    def _create_core_tables(self):
        """Creates the core tables for the application."""
        print("  - Creating core tables...")

        # Table: perp_data
        perp_data_query = """
        CREATE TABLE IF NOT EXISTS perp_data (
            ts BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            o NUMERIC(20, 8),
            h NUMERIC(20, 8),
            l NUMERIC(20, 8),
            c NUMERIC(20, 8),
            v NUMERIC(20, 8),
            oi NUMERIC(20, 8),
            pfr NUMERIC(20, 8),
            lsr NUMERIC(20, 8),
            tbv NUMERIC(20, 8),
            tsv NUMERIC(20, 8),
            rsi NUMERIC(10, 4),
            lql NUMERIC(20, 8),
            lqs NUMERIC(20, 8),
            PRIMARY KEY (ts, symbol)
        );
        """
        self.execute_query(perp_data_query)
        print("    - Created table: perp_data")

        # Table: perp_metrics
        perp_metrics_query = """
        CREATE TABLE IF NOT EXISTS perp_metrics (
            ts BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            -- Raw data columns for backtesting performance
            o NUMERIC(20, 8), h NUMERIC(20, 8), l NUMERIC(20, 8), c NUMERIC(20, 8), v NUMERIC(20, 8),
            oi NUMERIC(20, 8), pfr NUMERIC(20, 8), lsr NUMERIC(20, 8), tbv NUMERIC(20, 8), tsv NUMERIC(20, 8),
            rsi NUMERIC(10, 4), lql NUMERIC(20, 8), lqs NUMERIC(20, 8),
            -- Calculated change columns
            o_chg_1m NUMERIC(7,3), h_chg_1m NUMERIC(7,3), l_chg_1m NUMERIC(7,3), c_chg_1m NUMERIC(7,3),
            v_chg_1m NUMERIC(7,3), oi_chg_1m NUMERIC(7,3), pfr_chg_1m NUMERIC(7,3), lsr_chg_1m NUMERIC(7,3),
            tbv_chg_1m NUMERIC(7,3), tsv_chg_1m NUMERIC(7,3), rsi_chg_1m NUMERIC(7,3), lql_chg_1m NUMERIC(7,3),
            lqs_chg_1m NUMERIC(7,3),
            o_chg_5m NUMERIC(7,3), h_chg_5m NUMERIC(7,3), l_chg_5m NUMERIC(7,3), c_chg_5m NUMERIC(7,3),
            v_chg_5m NUMERIC(7,3), oi_chg_5m NUMERIC(7,3), pfr_chg_5m NUMERIC(7,3), lsr_chg_5m NUMERIC(7,3),
            tbv_chg_5m NUMERIC(7,3), tsv_chg_5m NUMERIC(7,3), rsi_chg_5m NUMERIC(7,3), lql_chg_5m NUMERIC(7,3),
            lqs_chg_5m NUMERIC(7,3),
            o_chg_10m NUMERIC(7,3), h_chg_10m NUMERIC(7,3), l_chg_10m NUMERIC(7,3), c_chg_10m NUMERIC(7,3),
            v_chg_10m NUMERIC(7,3), oi_chg_10m NUMERIC(7,3), pfr_chg_10m NUMERIC(7,3), lsr_chg_10m NUMERIC(7,3),
            tbv_chg_10m NUMERIC(7,3), tsv_chg_10m NUMERIC(7,3), rsi_chg_10m NUMERIC(7,3), lql_chg_10m NUMERIC(7,3),
            lqs_chg_10m NUMERIC(7,3),
            PRIMARY KEY (ts, symbol)
        );
        """
        self.execute_query(perp_metrics_query)
        print("    - Created table: perp_metrics")

        # Table: perp_status
        perp_status_query = """
        CREATE TABLE IF NOT EXISTS perp_status (
          task_id SERIAL PRIMARY KEY,
          script_name TEXT NOT NULL,
          status TEXT NOT NULL,
          message TEXT,
          details JSONB,
          ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        self.execute_query(perp_status_query)
        print("    - Created table: perp_status")

        # Table: perp_errors
        perp_errors_query = """
        CREATE TABLE IF NOT EXISTS perp_errors (
          error_id SERIAL PRIMARY KEY,
          script_name TEXT,
          error_type TEXT,
          error_message TEXT,
          details JSONB,
          ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        self.execute_query(perp_errors_query)
        print("    - Created table: perp_errors")

        # Placeholder Table: combo_algos
        combo_algos_query = """
        CREATE TABLE IF NOT EXISTS combo_algos (
            algo_id SERIAL PRIMARY KEY,
            algo_string TEXT NOT NULL,
            description TEXT,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        self.execute_query(combo_algos_query)
        print("    - Created placeholder table: combo_algos")

    def _setup_hypertables_and_policies(self):
        """Converts tables to hypertables and sets retention policies."""
        print("  - Setting up hypertables and retention policies...")

        # Define the integer_now_ms function for BIGINT timestamps
        function_query = """
        CREATE OR REPLACE FUNCTION integer_now_ms() RETURNS BIGINT LANGUAGE SQL STABLE AS $$
            SELECT CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT);
        $$;
        """
        self.execute_query(function_query)

        tables_to_hypertable = ["perp_data", "perp_metrics"]
        retention_ms = DB_RETENTION_DAYS * 24 * 60 * 60 * 1000

        for table in tables_to_hypertable:
            # Convert to hypertable
            hypertable_query = (
                f"SELECT create_hypertable('{table}', 'ts', if_not_exists => TRUE);"
            )
            self.execute_query(hypertable_query)

            # Set integer_now function
            set_integer_now_query = (
                f"SELECT set_integer_now_func('{table}', 'integer_now_ms');"
            )
            self.execute_query(set_integer_now_query)

            # Remove any existing policy before adding a new one
            remove_policy_query = (
                f"SELECT remove_retention_policy('{table}', if_exists => TRUE);"
            )
            self.execute_query(remove_policy_query)

            # Add new retention policy
            add_policy_query = (
                f"SELECT add_retention_policy('{table}', drop_after => {retention_ms});"
            )
            self.execute_query(add_policy_query)

            # Create indexes for performance
            table_identifier = sql.Identifier(table)
            index_ts_name = sql.Identifier(f"idx_{table}_ts")
            index_symbol_name = sql.Identifier(f"idx_{table}_symbol")

            index_ts_query = sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (ts DESC);"
            ).format(index_name=index_ts_name, table_name=table_identifier)
            index_symbol_query = sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (symbol);"
            ).format(index_name=index_symbol_name, table_name=table_identifier)
            self.execute_query(index_ts_query)
            self.execute_query(index_symbol_query)

            print(
                f"    - Configured hypertable, indexes, and {DB_RETENTION_DAYS}-day retention for: {table}"
            )


# ============================================================================
# STANDALONE EXECUTION BLOCK
# This allows the script to be run directly to set up the database.
# ============================================================================
if __name__ == "__main__":
    db_manager = DBManager()
    if db_manager.conn:
        db_manager.setup_database()
        db_manager.close_connection()
