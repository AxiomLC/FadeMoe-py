# server.py. rev:6Dec 2025 ver:1; 
# Unified server for API endpoints and frontend serving
"""
Server Notes (Dec 2025)

- Duplicate /health endpoint fixed: the generic health check was renamed to /status
  so the database-aware /health route works correctly again.

- Minor recommended improvements (not critical):
    • Enable autocommit on psycopg2 connections to avoid idle transaction edge cases.
    • Capture cursor.description before closing the cursor to prevent metadata errors.
    • Sanitize `params` handling in /api/perp_data to avoid empty or invalid fields.

- Future Revamp Suggested:
    Replace psycopg2 with asyncpg + a small connection pool for far higher throughput
    and lower latency. This would also allow the entire API layer to be fully async.
"""

import os
from contextlib import contextmanager
from typing import Generator, Optional
import psycopg2
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg2.extensions import connection as PsycopgConnection
from fastapi import Request
import asyncio
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection settings
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "1q2w3er4")
    DB_HOST = os.getenv("DB_HOST", "db")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "fademoe5")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Database connection context manager
@contextmanager
def get_db() -> Generator[Optional[PsycopgConnection], None, None]:
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True  # prevent idle transactions
    except Exception as e:
        print(f"DB connection failed: {e}")
    try:
        yield conn
    finally:
        if conn is not None:
            conn.close()

# API Endpoints
@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/health")
def health():
    try:
        with get_db() as db:
            if db is None:
                return {"status": "unhealthy", "reason": "Connection failed"}
            cur = db.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "reason": str(e)}

# Get all unique symbols
@app.get("/api/symbols")
async def get_symbols():
    try:
        with get_db() as db:
            if db is None:
                return JSONResponse(content={"error": "Connection failed"}, status_code=500)
            cur = db.cursor()
            cur.execute('SELECT DISTINCT symbol FROM perp_data ORDER BY symbol')
            rows = cur.fetchall()
            cur.close()
            symbols = [row[0] for row in rows]
            return {"symbols": symbols}
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return JSONResponse(content={"error": "Failed to fetch symbols"}, status_code=500)

# Get all unique exchanges
@app.get("/api/exchanges")
async def get_exchanges():
    try:
        with get_db() as db:
            if db is None:
                return JSONResponse(content={"error": "Connection failed"}, status_code=500)
            cur = db.cursor()
            cur.execute('SELECT DISTINCT exchange FROM perp_data ORDER BY exchange')
            rows = cur.fetchall()
            cur.close()
            exchanges = [row[0] for row in rows]
            return {"exchanges": exchanges}
    except Exception as e:
        print(f"Error fetching exchanges: {e}")
        return JSONResponse(content={"error": "Failed to fetch exchanges"}, status_code=500)

# Params list
@app.get("/api/params")
async def get_params():
    try:
        params = [
            'ts', 'symbol', 'exchange',
            'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 
            'rsi1', 'rsi60', 'tbv', 'tsv', 'lql', 'lqs'
        ]
        return {"params": params}
    except Exception as e:
        print(f"Error fetching params: {e}")
        return JSONResponse(content={"error": "Failed to fetch params"}, status_code=500)

# Get paginated perp_data
@app.get("/api/perp_data")
async def get_perp_data(
    page: int = 1,
    limit: int = 100,
    symbol: str = '',
    exchange: str = '',
    params: str = ''
):
    try:
        with get_db() as db:
            if db is None:
                return JSONResponse(content={"error": "Connection failed"}, status_code=500)
            
            pageNum = page
            pageSize = limit
            offset = (pageNum - 1) * pageSize

            where_clause = ''
            values = []

            # symbol filter
            if symbol and symbol != '' and symbol != 'all':
                symbols = [s.strip() for s in symbol.split(',') if len(s.strip()) > 0]
                if len(symbols) > 0:
                    placeholders = ','.join([f'%s'] * len(symbols))
                    where_clause += f" AND symbol IN ({placeholders})"
                    values.extend(symbols)

            # exchange filter
            if exchange and exchange != '' and exchange != 'all':
                exchanges = [e.strip() for e in exchange.split(',') if len(e.strip()) > 0]
                if len(exchanges) > 0:
                    placeholders = ','.join([f'%s'] * len(exchanges))
                    where_clause += f" AND exchange IN ({placeholders})"
                    values.extend(exchanges)

            valid_params = ['ts','symbol','exchange','o','h','l','c','v','oi','pfr','lsr','rsi1','rsi60','tbv','tsv','lql','lqs']

            select_fields = ', '.join(valid_params)
            visible_columns = valid_params
            
            if params and params != '' and params != 'all':
                selected_params = params.split(',')
                filtered_params = [param.strip() for param in selected_params if param.strip() in valid_params]
                if len(filtered_params) > 0:
                    select_fields = ', '.join(filtered_params)
                    visible_columns = filtered_params

            # COUNT query
            count_query = f"SELECT COUNT(*) FROM perp_data WHERE 1=1 {where_clause}"
            cur = db.cursor()
            cur.execute(count_query, values)
            colnames = [desc[0] for desc in cur.description]
            total_records = int(cur.fetchone()[0])
            cur.close()

            totalPages = (total_records + pageSize - 1) // pageSize

            # Main query
            values.extend([pageSize, offset])
            data_query = f"""
                SELECT {select_fields}
                FROM perp_data
                WHERE 1=1 {where_clause}
                ORDER BY ts DESC, symbol, exchange
                LIMIT %s OFFSET %s
            """
            cur = db.cursor()
            cur.execute(data_query, values)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            
            # Process rows
            processed_data = []
            for row in rows:
                rowdict = {}
                for col, val in zip(colnames, row):
                    if col == 'ts' and val is not None:
                        rowdict[col] = int(val)
                    else:
                        rowdict[col] = val
                processed_data.append(rowdict)

            return {
                "data": processed_data,
                "pagination": {
                    "currentPage": pageNum,
                    "totalPages": totalPages,
                    "totalRecords": total_records,
                    "pageSize": pageSize
                },
                "visibleColumns": visible_columns
            }

    except Exception as e:
        print(f"Error fetching perp_data: {e}")
        return JSONResponse(content={"error": "Failed to fetch perp_data"}, status_code=500)

# latest metrics
@app.get("/api/latest-metrics")
async def get_latest_metrics(symbols: str = '', exchanges: str = ''):
    try:
        with get_db() as db:
            if db is None:
                return JSONResponse(content={"error": "Connection failed"}, status_code=500)
                
            symbol_list = [s.strip().upper() for s in symbols.split(',') if s.strip()]
            exchange_list = [e.strip() for e in exchanges.split(',') if e.strip()]

            if not symbol_list or not exchange_list:
                return JSONResponse(content={"error": "Symbols and exchanges query parameters are required"}, status_code=400)

            values = []
            where_clause = ''

            if symbol_list:
                placeholders = ','.join(['%s'] * len(symbol_list))
                where_clause += f" AND symbol IN ({placeholders})"
                values.extend(symbol_list)

            if exchange_list:
                placeholders = ','.join(['%s'] * len(exchange_list))
                where_clause += f" AND exchange IN ({placeholders})"
                values.extend(exchange_list)

            query = f"""
                SELECT pm.*
                FROM perp_metrics pm
                INNER JOIN (
                    SELECT symbol, exchange, MAX(ts) AS max_ts
                    FROM perp_metrics
                    WHERE 1=1 {where_clause}
                    GROUP BY symbol, exchange
                ) latest
                ON pm.symbol = latest.symbol
                AND pm.exchange = latest.exchange
                AND pm.ts = latest.max_ts
                ORDER BY pm.symbol, pm.exchange
            """

            cur = db.cursor()
            cur.execute(query, values)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            
            processed_data = []
            for row in rows:
                d = {}
                for col, val in zip(colnames, row):
                    if col == "ts" and val is not None:
                        d[col] = int(val)
                    else:
                        d[col] = val
                processed_data.append(d)

            return {"data": processed_data}

    except Exception as e:
        print(f"Error fetching latest metrics: {e}")
        return JSONResponse(content={"error": "Failed to fetch latest metrics"}, status_code=500)

# system summary
@app.get("/api/system-summary")
async def get_system_summary():
    try:
        with get_db() as db:
            if db is None:
                return JSONResponse(content={"error": "Connection failed"}, status_code=500)
                
            # latest status
            status_query = """
                SELECT script_name, status, message, ts 
                FROM perp_status 
                ORDER BY ts DESC 
                LIMIT 10
            """
            cur = db.cursor()
            cur.execute(status_query)
            latest_status_rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            cur.close()

            running_scripts = []
            recent_status = []
            if latest_status_rows:
                all_rows = [dict(zip(colnames, row)) for row in latest_status_rows]
                running_scripts = [r['script_name'] for r in all_rows if r.get('status') == 'running']
                recent_status = all_rows[:5]

            # recent errors
            errors_query = """
                SELECT script_name, error_type, error_message, ts 
                FROM perp_errors 
                ORDER BY ts DESC 
                LIMIT 10
            """
            cur = db.cursor()
            cur.execute(errors_query)
            error_rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            
            error_rows = [dict(zip(colnames, row)) for row in error_rows]

            # Build HTML summary
            status_text = '<div id="status-flex-container" style="display:flex; gap:20px; max-height:250px; background-color:#1e1e2f; padding:0; border-radius:8px; color:#d1d5db; overflow:hidden;">'

            status_text += '<div id="status-left" style="flex:1 1 60%; background-color:#000; padding:5px; border:none; overflow-y:auto; max-height:200px;">'
            status_text += '<div style="margin-bottom: 15px;">'
            status_text += '<h3 style="color: #9f59ff; margin: 0 0 10px 0;">📊 Current Operations</h3>'
            
            if running_scripts:
                for script in running_scripts:
                    status_text += f'<div style="padding-left: 15px; margin-top: 3px; color: #fbbf24; font-size: 14px;">• {script}</div>'
            else:
                status_text += '<div style="color: #d1d5db;">No scripts currently running.</div>'
            
            status_text += '</div>'

            if recent_status:
                status_text += '<div style="margin-bottom: 15px;">'
                status_text += '<strong style="color: #b569ff;">Latest Activity:</strong>'
                for op in recent_status:
                    status_color = '#d1d5db'
                    if op.get('status') == 'success':
                        status_color = '#4ade80'
                    elif op.get('status') == 'running':
                        status_color = '#fbbf24'
                    elif op.get('status') == 'error':
                        status_color = '#f87171'

                    if op.get('ts'):
                        ts = datetime.fromisoformat(op['ts'].replace('Z', '+00:00'))
                        time_str = ts.strftime('%H:%M:%S') + ' UTC'
                        status_text += f'<div style="padding-left: 15px; margin-top: 3px; color: {status_color}; font-size: 14px;">'
                        status_text += f'• {op.get("script_name", "Unknown")} [{time_str}]: {op.get("message", op.get("status", "No message"))}'
                        status_text += '</div>'
                status_text += '</div>'

            status_text += '</div>'  # end left

            status_text += '<div id="status-right" style="flex:1 1 40%; background-color:#000; border:none; border-left: 1px solid rgba(159, 89, 255, 0.3); padding:5px; overflow-y:auto; max-height:200px;">'

            if error_rows:
                status_text += '<h4 style="color: #f87171; margin: 0 0 10px 0;">⚠️ Recent Issues</h4>'
                for error in error_rows[:3]:
                    if error.get('ts'):
                        ts = datetime.fromisoformat(error['ts'].replace('Z', '+00:00'))
                        time_str = ts.strftime('%H:%M:%S') + ' UTC'
                        status_text += f'<div style="margin-bottom: 8px; padding-left: 10px;">'
                        status_text += f'<strong style="color: #fbbf24;">{error.get("script_name", "Unknown")} [{time_str}]:</strong> {error.get("error_message", "Unknown error")}<br>'
                        status_text += '</div>'
            else:
                status_text += '<div style="color: #d1d5db;">No recent issues.</div>'

            status_text += '</div>'  # end right
            status_text += '</div>'  # container

            if not running_scripts and not recent_status and not error_rows:
                status_text = '<div style="text-align: center; padding: 20px;">'
                status_text += '<h3 style="color: #9f59ff; margin: 0 0 10px 0;">🌟 System Ready</h3>'
                status_text += '<p style="color: #d1d5db; margin: 0;">No recent activity. Data collection scripts are ready to run.</p>'
                status_text += '</div>'

            return {
                "statusText": status_text,
                "errors": error_rows,
                "operations": recent_status
            }

    except Exception as e:
        print(f"API Error: Failed to fetch system summary: {e}")
        return JSONResponse(content={"error": "Failed to fetch system summary"}, status_code=500)

# Status endpoint
@app.get("/status")
async def status_check():
    return {"status": "OK", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
