# back/dbsetup2.py. rev:11Dec 2025 ver:2; Combined async-logger COPY architecture
# --------------------------------------------------------------
# ALL tables, indexes, hypertables + COPY staging
# WITH async logging support for api_utils.py
# --------------------------------------------------------------

import psycopg2
import psycopg2.pool
import psycopg2.extras
import io
import os
import asyncio

try:
    from back.perp_input import DB_RETENTION_DAYS
except ImportError:
    DB_RETENTION_DAYS = 20

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "perpdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

class DBManager:
    def __init__(self):
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=3,
                maxconn=25,
                **DB_CONFIG
            )
            self.conn = self.pool.getconn()
            self._merge_lock = asyncio.Lock()
            print("✅ Database connection pool created.")
        except Exception as e:
            print(f"❌ Failed to initialize DB: {e}")
            self.pool = None
            self.conn = None

    # --------------------------------------------------------------------
    # Setup Methods (async.to_thread compatible)
    # --------------------------------------------------------------------
    async def setup_database(self):
        """Setup DB with async wrapper."""
        await asyncio.to_thread(self._setup_database_sync)
    
    def _setup_database_sync(self):
        print("⚙️ Setting up database...")
        self._drop_existing_tables_sync()
        psycopg2.extras.execute_batch(self.conn.cursor(), "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE", [])
        self._create_core_tables_sync()
        self._create_staging_tables_sync()
        self._setup_hypertables_sync()
        print("✅ Database setup complete.")
    
    def _drop_existing_tables_sync(self):
        tables = ["perp_data","perp_metrics","perp_status","perp_errors","combo_algos","perp_data_stage","perp_metrics_stage"]
        for table in tables:
            self.conn.cursor().execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

    def _create_core_tables_sync(self):
        self.conn.cursor().execute("""
            CREATE TABLE perp_data (
                ts BIGINT NOT NULL, symbol TEXT NOT NULL,
                o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
                v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
                tbv NUMERIC(20,8), tsv NUMERIC(20,8), rsi NUMERIC(10,4),
                lql NUMERIC(20,8), lqs NUMERIC(20,8),
                PRIMARY KEY (ts, symbol)
            );
        """)
        
        self.conn.cursor().execute("""
            CREATE TABLE perp_metrics (
                ts BIGINT NOT NULL, symbol TEXT NOT NULL,
                o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
                v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
                tbv NUMERIC(20,8), tsv NUMERIC(20,8), rsi NUMERIC(10,4),
                lql NUMERIC(20,8), lqs NUMERIC(20,8),
                o_chg_1m NUMERIC(7,3), h_chg_1m NUMERIC(7,3), l_chg_1m NUMERIC(7,3),
                c_chg_1m NUMERIC(7,3), v_chg_1m NUMERIC(7,3), oi_chg_1m NUMERIC(7,3),
                pfr_chg_1m NUMERIC(7,3), lsr_chg_1m NUMERIC(7,3),
                tbv_chg_1m NUMERIC(7,3), tsv_chg_1m NUMERIC(7,3),
                rsi_chg_1m NUMERIC(7,3), lql_chg_1m NUMERIC(7,3), lqs_chg_1m NUMERIC(7,3),
                o_chg_5m NUMERIC(7,3), h_chg_5m NUMERIC(7,3), l_chg_5m NUMERIC(7,3),
                c_chg_5m NUMERIC(7,3), v_chg_5m NUMERIC(7,3), oi_chg_5m NUMERIC(7,3),
                pfr_chg_5m NUMERIC(7,3), lsr_chg_5m NUMERIC(7,3),
                tbv_chg_5m NUMERIC(7,3), tsv_chg_5m NUMERIC(7,3),
                rsi_chg_5m NUMERIC(7,3), lql_chg_5m NUMERIC(7,3), lqs_chg_5m NUMERIC(7,3),
                o_chg_10m NUMERIC(7,3), h_chg_10m NUMERIC(7,3), l_chg_10m NUMERIC(7,3),
                c_chg_10m NUMERIC(7,3), v_chg_10m NUMERIC(7,3), oi_chg_10m NUMERIC(7,3),
                pfr_chg_10m NUMERIC(7,3), lsr_chg_10m NUMERIC(7,3),
                tbv_chg_10m NUMERIC(7,3), tsv_chg_10m NUMERIC(7,3),
                rsi_chg_10m NUMERIC(7,3), lql_chg_10m NUMERIC(7,3), lqs_chg_10m NUMERIC(7,3),
                PRIMARY KEY (ts, symbol)
            );
        """)
        # perp_status, perp_errors, combo_algos remain same
        self.conn.cursor().execute("""
            CREATE TABLE perp_status (
                ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                task_id SERIAL, script_name TEXT NOT NULL,
                status TEXT NOT NULL, message TEXT, details JSONB,
                PRIMARY KEY (ts, task_id, script_name)
            );
            CREATE INDEX idx_perp_status_ts ON perp_status (ts DESC);
        """)
        self.conn.cursor().execute("""
            CREATE TABLE perp_errors (
                ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                error_id SERIAL, script_name TEXT NOT NULL,
                error_type TEXT, error_message TEXT, details JSONB,
                PRIMARY KEY (ts, error_id, script_name)
            );
            CREATE INDEX idx_perp_errors_ts ON perp_errors (ts DESC);
        """)
        self.conn.cursor().execute("""
            CREATE TABLE combo_algos (
                algo_id SERIAL PRIMARY KEY, algo_string TEXT NOT NULL,
                description TEXT, is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        self.conn.commit()

    def _create_staging_tables_sync(self):
        # Staging table schema exactly matches perp_data/perp_metrics
        self.conn.cursor().execute("""
            CREATE TABLE perp_data_stage (
                ts BIGINT, symbol TEXT,
                o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
                v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
                tbv NUMERIC(20,8), tsv NUMERIC(20,8), rsi NUMERIC(10,4),
                lql NUMERIC(20,8), lqs NUMERIC(20,8)
            );
        """)
        self.conn.cursor().execute("""
            CREATE TABLE perp_metrics_stage (
                ts BIGINT, symbol TEXT,
                o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
                v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
                tbv NUMERIC(20,8), tsv NUMERIC(20,8), rsi NUMERIC(10,4),
                lql NUMERIC(20,8), lqs NUMERIC(20,8),
                o_chg_1m NUMERIC(7,3), h_chg_1m NUMERIC(7,3), l_chg_1m NUMERIC(7,3),
                c_chg_1m NUMERIC(7,3), v_chg_1m NUMERIC(7,3), oi_chg_1m NUMERIC(7,3),
                pfr_chg_1m NUMERIC(7,3), lsr_chg_1m NUMERIC(7,3),
                tbv_chg_1m NUMERIC(7,3), tsv_chg_1m NUMERIC(7,3),
                rsi_chg_1m NUMERIC(7,3), lql_chg_1m NUMERIC(7,3), lqs_chg_1m NUMERIC(7,3),
                o_chg_5m NUMERIC(7,3), h_chg_5m NUMERIC(7,3), l_chg_5m NUMERIC(7,3),
                c_chg_5m NUMERIC(7,3), v_chg_5m NUMERIC(7,3), oi_chg_5m NUMERIC(7,3),
                pfr_chg_5m NUMERIC(7,3), lsr_chg_5m NUMERIC(7,3),
                tbv_chg_5m NUMERIC(7,3), tsv_chg_5m NUMERIC(7,3),
                rsi_chg_5m NUMERIC(7,3), lql_chg_5m NUMERIC(7,3), lqs_chg_5m NUMERIC(7,3),
                o_chg_10m NUMERIC(7,3), h_chg_10m NUMERIC(7,3), l_chg_10m NUMERIC(7,3),
                c_chg_10m NUMERIC(7,3), v_chg_10m NUMERIC(7,3), oi_chg_10m NUMERIC(7,3),
                pfr_chg_10m NUMERIC(7,3), lsr_chg_10m NUMERIC(7,3),
                tbv_chg_10m NUMERIC(7,3), tsv_chg_10m NUMERIC(7,3),
                rsi_chg_10m NUMERIC(7,3), lql_chg_10m NUMERIC(7,3), lqs_chg_10m NUMERIC(7,3)
            );
        """)
        self.conn.commit()

    def _setup_hypertables_sync(self):
        self.conn.cursor().execute("""
            CREATE OR REPLACE FUNCTION integer_now_ms() RETURNS BIGINT LANGUAGE SQL STABLE AS $$
                SELECT CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT);
            $$;
        """)
        tables = ["perp_data", "perp_metrics"]
        retention_ms = DB_RETENTION_DAYS * 24 * 60 * 60 * 1000
        for table in tables:
            try:
                self.conn.cursor().execute(f"SELECT create_hypertable('{table}', 'ts', if_not_exists => TRUE);")
                self.conn.cursor().execute(f"SELECT set_integer_now_func('{table}', 'integer_now_ms');")
                self.conn.cursor().execute(f"SELECT remove_retention_policy('{table}', if_exists => TRUE);")
                self.conn.cursor().execute(f"SELECT add_retention_policy('{table}', drop_after => {retention_ms});")
                self.conn.cursor().execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table} (ts DESC);")
                self.conn.cursor().execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {table} (symbol);")
            except Exception as e:
                print(f"⚠️ Hypertable setup for {table} skipped: {e}")
        self.conn.commit()

    # --------------------------------------------------------------------
    # FLEXIBLE MERGING STAGING (supports multiple API types)
    # --------------------------------------------------------------------
    async def merge_data_to_stage(self, *batches):
        """
        Merge multiple batches from different API types into staging.
        *batches: lists of dicts from ohlcv, pfr, rsi, tv, etc.
        This function will batch input, perform COPY in chunks,
        and perform a single final merge + clear operation atomically.
        Returns total inserted record count.
        """
        if not batches:
            return 0

        # Step 1: Flatten all batches
        all_rows = []
        for batch in batches:
            if batch:
                all_rows.extend(batch)

        if not all_rows:
            return 0

        # Step 2: Merge by (ts, symbol) preserving non-NULL values
        merged_dict = {}
        full_cols = ["ts","symbol","o","h","l","c","v","oi","pfr","lsr","tbv","tsv","rsi","lql","lqs"]
        for row in all_rows:
            key = (row.get('ts'), row.get('symbol'))
            if key not in merged_dict:
                merged_dict[key] = {}
            dest = merged_dict[key]
            for col in full_cols:
                val = row.get(col)
                if val is not None and val != "":
                    dest[col] = val
                elif col not in dest:
                    dest[col] = None

        merged_rows = list(merged_dict.values())

        # Step 3 & 4: Batch COPY + Single atomic merge and clear stage inside lock
        async with self._merge_lock:
            total_inserted = 0
            batch_size = 4000  # Reasonable batch size for memory
            for i in range(0, len(merged_rows), batch_size):
                chunk = merged_rows[i:i + batch_size]
                inserted = await asyncio.to_thread(self._copy_to_stage_sync, chunk)
                if inserted is None:
                    # Copy failed, continue
                    continue
                total_inserted += inserted

            await self.merge_stage()
            await self.clear_stage()

        return total_inserted
    
    def _copy_to_stage_sync(self, rows):
        """COPY rows into perp_data_stage (sync version)."""
        if not rows: return 0

        columns = ["ts","symbol","o","h","l","c","v","oi","pfr","lsr","tbv","tsv","rsi","lql","lqs"]
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            buf = io.StringIO()
            row_count = 0
            for r in rows:
                # Check if row has at least one meaningful param column filled (include tbv/tsv as meaningful)
                meaningful_cols = ["o","h","l","c","v","oi","pfr","lsr","rsi","lql","lqs","tbv","tsv"]
                if any(r.get(col) not in [None, "", "\n"] for col in meaningful_cols):
                    line = "\t".join(r"\N" if (r.get(col) is None or r.get(col) == "" or str(r.get(col)).strip() == "") else str(r.get(col)) for col in columns)
                    buf.write(line + "\n")
                    row_count += 1
                else:
                    print(f"[Warning] Skipping row with only non-meaningful columns: ts={r.get('ts')}, symbol={r.get('symbol')}")
            if row_count == 0:
                print("[Info] No meaningful rows to copy to perp_data_stage.")
                return 0
            buf.seek(0)
            cur.copy_from(buf, "perp_data_stage", sep="\t", columns=columns, null="\\N")
            conn.commit()
            return row_count
        except Exception as e:
            print(f"❌ COPY to perp_data_stage failed: {e}")
            conn.rollback()
            return 0
        finally:
            self.pool.putconn(conn)

    async def merge_stage(self):
        """Async wrapper for MERGE staging."""
        await asyncio.to_thread(self._merge_stage_sync)
    
    async def merge_stage(self):
        """Async wrapper for MERGE staging."""
        await asyncio.to_thread(self._merge_stage_sync)
    
    def _merge_stage_sync(self):
        """Merge perp_data_stage into perp_data (sync version)."""
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO perp_data AS t
                SELECT * FROM perp_data_stage
                ON CONFLICT (ts, symbol)
                DO UPDATE SET
                    o = COALESCE(EXCLUDED.o, t.o),
                    h = COALESCE(EXCLUDED.h, t.h),
                    l = COALESCE(EXCLUDED.l, t.l),
                    c = COALESCE(EXCLUDED.c, t.c),
                    v = COALESCE(EXCLUDED.v, t.v),
                    oi = COALESCE(EXCLUDED.oi, t.oi),
                    pfr = COALESCE(EXCLUDED.pfr, t.pfr),
                    lsr = COALESCE(EXCLUDED.lsr, t.lsr),
                    tbv = COALESCE(EXCLUDED.tbv, t.tbv),
                    tsv = COALESCE(EXCLUDED.tsv, t.tsv),
                    rsi = COALESCE(EXCLUDED.rsi, t.rsi),
                    lql = COALESCE(EXCLUDED.lql, t.lql),
                    lqs = COALESCE(EXCLUDED.lqs, t.lqs);
            """)
            conn.commit()
        except Exception as e:
            print(f"❌ merge_stage failed: {e}")
            conn.rollback()
        finally:
            self.pool.putconn(conn)
    # --------------------------------------------------------------------
    # PERP_METRICS STAGING METHODS (for calc_metrics.py)
    # --------------------------------------------------------------------
    async def copy_to_metrics_stage(self, rows):
        await asyncio.to_thread(self._copy_to_metrics_stage_sync, rows)
    
    def _copy_to_metrics_stage_sync(self, rows):
        if not rows: return
        columns = [
            "ts","symbol","o","h","l","c","v","oi","pfr","lsr","tbv","tsv","rsi","lql","lqs",
            "o_chg_1m","h_chg_1m","l_chg_1m","c_chg_1m","v_chg_1m","oi_chg_1m","pfr_chg_1m","lsr_chg_1m",
            "tbv_chg_1m","tsv_chg_1m","rsi_chg_1m","lql_chg_1m","lqs_chg_1m",
            "o_chg_5m","h_chg_5m","l_chg_5m","c_chg_5m","v_chg_5m","oi_chg_5m","pfr_chg_5m","lsr_chg_5m",
            "tbv_chg_5m","tsv_chg_5m","rsi_chg_5m","lql_chg_5m","lqs_chg_5m",
            "o_chg_10m","h_chg_10m","l_chg_10m","c_chg_10m","v_chg_10m","oi_chg_10m","pfr_chg_10m","lsr_chg_10m",
            "tbv_chg_10m","tsv_chg_10m","rsi_chg_10m","lql_chg_10m","lqs_chg_10m"
        ]
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            buf = io.StringIO()
            for r in rows:
                line = "\t".join(r"\N" if (r.get(col) is None or r.get(col) == "" or str(r.get(col)).strip() == "") else str(r.get(col)) for col in columns)
                buf.write(line + "\n")
            buf.seek(0)
            cur.copy_from(buf, "perp_metrics_stage", sep="\t", columns=columns, null="\\N")
            conn.commit()
        except Exception as e:
            print(f"❌ COPY to perp_metrics_stage failed: {e}")
            conn.rollback()
        finally:
            self.pool.putconn(conn)

    async def merge_metrics_stage(self):
        await asyncio.to_thread(self._merge_metrics_stage_sync)
    
    def _merge_metrics_stage_sync(self):
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO perp_metrics AS t
                SELECT * FROM perp_metrics_stage
                ON CONFLICT (ts, symbol)
                DO UPDATE SET
                    o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c,
                    v = EXCLUDED.v, oi = EXCLUDED.oi, pfr = EXCLUDED.pfr, lsr = EXCLUDED.lsr,
                    tbv = EXCLUDED.tbv, tsv = EXCLUDED.tsv, rsi = EXCLUDED.rsi,
                    lql = EXCLUDED.lql, lqs = EXCLUDED.lqs,
                    o_chg_1m = EXCLUDED.o_chg_1m, h_chg_1m = EXCLUDED.h_chg_1m, l_chg_1m = EXCLUDED.l_chg_1m,
                    c_chg_1m = EXCLUDED.c_chg_1m, v_chg_1m = EXCLUDED.v_chg_1m, oi_chg_1m = EXCLUDED.oi_chg_1m,
                    pfr_chg_1m = EXCLUDED.pfr_chg_1m, lsr_chg_1m = EXCLUDED.lsr_chg_1m,
                    tbv_chg_1m = EXCLUDED.tbv_chg_1m, tsv_chg_1m = EXCLUDED.tsv_chg_1m,
                    rsi_chg_1m = EXCLUDED.rsi_chg_1m, lql_chg_1m = EXCLUDED.lql_chg_1m, lqs_chg_1m = EXCLUDED.lqs_chg_1m,
                    o_chg_5m = EXCLUDED.o_chg_5m, h_chg_5m = EXCLUDED.h_chg_5m, l_chg_5m = EXCLUDED.l_chg_5m,
                    c_chg_5m = EXCLUDED.c_chg_5m, v_chg_5m = EXCLUDED.v_chg_5m, oi_chg_5m = EXCLUDED.oi_chg_5m,
                    pfr_chg_5m = EXCLUDED.pfr_chg_5m, lsr_chg_5m = EXCLUDED.lsr_chg_5m,
                    tbv_chg_5m = EXCLUDED.tbv_chg_5m, tsv_chg_5m = EXCLUDED.tsv_chg_5m,
                    rsi_chg_5m = EXCLUDED.rsi_chg_5m, lql_chg_5m = EXCLUDED.lql_chg_5m, lqs_chg_5m = EXCLUDED.lqs_chg_5m,
                    o_chg_10m = EXCLUDED.o_chg_10m, h_chg_10m = EXCLUDED.h_chg_10m, l_chg_10m = EXCLUDED.l_chg_10m,
                    c_chg_10m = EXCLUDED.c_chg_10m, v_chg_10m = EXCLUDED.v_chg_10m, oi_chg_10m = EXCLUDED.oi_chg_10m,
                    pfr_chg_10m = EXCLUDED.pfr_chg_10m, lsr_chg_10m = EXCLUDED.lsr_chg_10m,
                    tbv_chg_10m = EXCLUDED.tbv_chg_10m, tsv_chg_10m = EXCLUDED.tsv_chg_10m,
                    rsi_chg_10m = EXCLUDED.rsi_chg_10m, lql_chg_10m = EXCLUDED.lql_chg_10m, lqs_chg_10m = EXCLUDED.lqs_chg_10m;
            """)
            conn.commit()
        except Exception as e:
            print(f"❌ merge_metrics_stage failed: {e}")
            conn.rollback()
        finally:
            self.pool.putconn(conn)

    async def clear_metrics_stage(self):
        await asyncio.to_thread(self._clear_metrics_stage_sync)
    
    def _clear_metrics_stage_sync(self):
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("TRUNCATE perp_metrics_stage;")
            conn.commit()
        except Exception as e:
            print(f"❌ clear_metrics_stage failed: {e}")
            conn.rollback()
        finally:
            self.pool.putconn(conn)

# --------------------------------------------------------------------
# LEGACY COMPATIBILITY (supports async.to_thread)
# --------------------------------------------------------------------
    async def insert_batch_data(self, rows, target="perp_data"):
        """Legacy slow insert for compatibility."""
        if not rows: return 0
        try:
            # Example: Implement your original insert logic here
            # This is a placeholder that returns success status
            return len(rows)
        except Exception as e:
            print(f"❌ Legacy insert_batch_data failed: {e}")
            return 0

# --------------------------------------------------------------------
# Connection management
# --------------------------------------------------------------------
    def close_connection(self):
        if self.pool:
            self.pool.closeall()

    async def clear_stage(self):
        await asyncio.to_thread(self._clear_stage_sync)

    def _clear_stage_sync(self):
        conn = self.pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("TRUNCATE perp_data_stage;")
            conn.commit()
        except Exception as e:
            print(f"❌ clear_stage failed: {e}")
            conn.rollback()
        finally:
            self.pool.putconn(conn)
#============== per CLAUDE 12 Dec =====================
    async def query_perp_data(self, symbol, start_ts, end_ts):
        """Query OHLCV data (ts, c, v) for a symbol and time range."""
        sql = """
        SELECT ts, c::float, v::float
        FROM perp_data
        WHERE symbol = %s AND ts >= %s AND ts < %s 
        AND c IS NOT NULL AND v IS NOT NULL
        ORDER BY ts ASC
        """
        return await asyncio.to_thread(self._query_perp_data_sync, sql, (symbol, start_ts, end_ts))
    
    def _query_perp_data_sync(self, sql, params):
        """Sync version of query_perp_data."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                if not rows:
                    return []
                return [{"ts": row[0], "c": row[1], "v": row[2]} for row in rows]
        finally:
            self.pool.putconn(conn)
    #=======================================================

    async def execute_query(self, query, params=None, fetch=None):
        """Async query wrapper for logging."""
        # Use threadpool to avoid blocking
        return await asyncio.to_thread(self._execute_query_sync, query, params, fetch)
    
    def _execute_query_sync(self, query, params=None, fetch=None):
        """Sync query (called via asyncio.to_thread)."""
        if not self.conn: return None
        conn = self.pool.getconn() if self.pool else self.conn
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                if fetch == "one": return cur.fetchone()
                if fetch == "all": return cur.fetchall()
        except psycopg2.Error as e:
            print(f"❌ Query failed: {e}")
            conn.rollback()
            return None
        finally:
            if self.pool:
                self.pool.putconn(conn)

# ========================================================================
# TEST / SETUP (for standalone execution)
# ========================================================================
async def run_full_setup_async():
    print("\n🚀 Starting DB setup...")
    db = DBManager()
    await db.setup_database()
    db.close_connection()
    print("✅ DB setup complete.")

def run_full_setup():
    """Sync version for direct execution."""
    import asyncio
    asyncio.run(run_full_setup_async())

if __name__ == "__main__":
    run_full_setup()