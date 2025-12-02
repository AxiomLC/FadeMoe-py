# back/db_display.py. rev:2Dec 2025 ver:3; corrected 'tv' logic to use 'tbv' and 'tsv' columns.
import os
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

# ============================================================================
#  1. USER CONTROLS
# ============================================================================
# Add or remove column names from this list to change the report.
# Valid columns: ohlcv, pfr, oi, lsr, rsi, tbv, tsv
COLUMNS_TO_VIEW = [
    "ohlcv",
    "pfr",
    "oi",
    "lsr",
    "rsi",
    "tbv",
    "tsv",
    "lql",
    "lqs",
]

# ============================================================================
#  2. SCRIPT SETUP
# ============================================================================
load_dotenv()

try:
    from back.perp_input import DB_RETENTION_DAYS
except ImportError:
    DB_RETENTION_DAYS = 30 # Fallback if not found

def get_db_connection():
    # ... (unchanged)
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
        )
        print("✅ Database connection successful.\n")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def run_query(conn, query, params=None):
    """
    BUG FIX: Executes a query using psycopg2's cursor and loads the result
    into pandas, avoiding the sqlalchemy conflict.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            if not results:
                return pd.DataFrame()
            
            # Convert list of DictRow objects to a list of dicts, then to DataFrame
            return pd.DataFrame([dict(row) for row in results])

    except Exception as e:
        print(f"❌ Query failed: {e}")
        return pd.DataFrame()

def build_select_clause(columns):
    """Dynamically builds the SELECT clause with division-by-zero protection."""
    clause = "COUNT(*) AS total_rows"
    denominator = "NULLIF(COUNT(*), 0)" # Prevents division by zero
    for col in columns:
        if col == "ohlcv":
            clause += f", ROUND((COUNT(c) * 100.0 / {denominator}), 2) AS ohlcv_pct"
        else: # For pfr, oi, lsr, rsi, tbv, tsv
            clause += f", ROUND((COUNT({col}) * 100.0 / {denominator}), 2) AS {col}_pct"
    return clause

# ============================================================================
#  3. MAIN EXECUTION
# ============================================================================
def main():
    conn = get_db_connection()
    if not conn: return

    start_ts_ms = (datetime.now(timezone.utc).timestamp() - (DB_RETENTION_DAYS * 24 * 60 * 60)) * 1000
    fifteen_minutes_ago_ms = (datetime.now(timezone.utc).timestamp() - 15 * 60) * 1000

    select_clause = build_select_clause(COLUMNS_TO_VIEW)

    # --- Query 1: Overall Completeness ---
    print(f"--- 1. Overall Data Completeness (Last {DB_RETENTION_DAYS} Days) ---")
    query1 = f"""SELECT {select_clause} FROM perp_data WHERE ts >= %(ts_ms)s;"""
    df1 = run_query(conn, query1, params={"ts_ms": start_ts_ms})
    if not df1.empty:
        print(df1.to_string(index=False))

    # --- Query 2: Data Freshness ---
    print("\n\n--- 2. Data Freshness (Last 15 Minutes) ---")
    query2 = f"""SELECT {select_clause} FROM perp_data WHERE ts >= %(ts_ms)s;"""
    df2 = run_query(conn, query2, params={"ts_ms": fifteen_minutes_ago_ms})
    if not df2.empty:
        print(df2.to_string(index=False))

    # --- Query 3: Fill Rate by Symbol ---
    print("\n\n--- 3. Fill Rate by Symbol (Top 10 by Row Count) ---")
    # This query also needs the division-by-zero protection in its SELECT clause
    select_clause_for_symbol = build_select_clause(COLUMNS_TO_VIEW).replace('COUNT(*) AS total_rows,', '')
    query3 = f"""
    WITH symbol_counts AS (
        SELECT symbol, COUNT(*) as row_count
        FROM perp_data WHERE ts >= %(ts_ms)s
        GROUP BY symbol ORDER BY row_count DESC LIMIT 10
    )
    SELECT p.symbol, sc.row_count, {select_clause_for_symbol}
    FROM perp_data p
    JOIN symbol_counts sc ON p.symbol = sc.symbol
    WHERE p.ts >= %(ts_ms)s
    GROUP BY p.symbol, sc.row_count ORDER BY sc.row_count DESC;
    """
    df3 = run_query(conn, query3, params={"ts_ms": start_ts_ms})
    if not df3.empty:
        print(df3.to_string(index=False))

    conn.close()
    print("\n🔌 Database connection closed.")

if __name__ == "__main__":
    main()
