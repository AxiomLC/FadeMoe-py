import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg2.extras import RealDictCursor  # For dict results

from back.dbsetup import DBManager  # Import existing DBManager (run dbsetup.py first)

load_dotenv()

app = FastAPI(title="FadeMoe5 Frontend")

# CORS for external sites
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static (for CSS if added)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Jinja2 templates (warns if files missing, but APIs work)
templates = Jinja2Templates(directory="views")

# Global DBManager (init in __init__)
db_manager = DBManager()


# Helper for queries (uses RealDictCursor for dict rows)
def query_db(sql: str, params: Optional[List] = None, fetch: str = "all") -> List:
    if not db_manager.conn:
        return []
    result = db_manager.execute_query(sql, params, fetch)
    return result or []


# API: Dynamic symbols from perp_metrics
@app.get("/api/symbols")
def get_symbols():
    try:
        symbols = query_db(
            "SELECT DISTINCT symbol FROM perp_metrics ORDER BY symbol", fetch="all"
        )
        return [row["symbol"] for row in symbols]  # Dict from RealDictCursor
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# API: Dynamic params (columns from perp_metrics, exclude ts/symbol)
@app.get("/api/params")
def get_params():
    try:
        params = query_db(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'perp_metrics' AND column_name NOT IN ('ts', 'symbol')
            ORDER BY ordinal_position
        """,
            fetch="all",
        )
        return [row["column_name"] for row in params]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# API: Paginated/filtered perp_metrics (shared for dbview/cards)
@app.get("/api/perp_metrics")
def get_perp_metrics(
    page: int = Query(1, ge=1),
    limit: int = Query(100, le=1000),
    symbol: str = Query(""),
    params: str = Query("all"),
):
    try:
        offset = (page - 1) * limit
        values = []
        where_parts = []
        if symbol and symbol != "all":
            syms = [s.strip() for s in symbol.split(",") if s.strip()]
            if syms:
                placeholders = ", ".join(
                    [f"${len(values) + i + 1}" for i in range(len(syms))]
                )
                where_parts.append(f"symbol IN ({placeholders})")
                values.extend(syms)
        where_clause = " AND " + " AND ".join(where_parts) if where_parts else ""
        # Fields
        if params == "all":
            fields = "*"  # All columns
        else:
            param_list = [p.strip() for p in params.split(",") if p.strip()]
            fields = ", ".join(param_list)
            fields = "ts, symbol, " + fields if param_list else "*"
        # Count
        count_sql = (
            f"SELECT COUNT(*) as count FROM perp_metrics WHERE 1=1{where_clause}"
        )
        count_result = query_db(count_sql, values, fetch="one")
        total = count_result["count"] if count_result else 0
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        # Data
        data_sql = f"""
            SELECT {fields} FROM perp_metrics WHERE 1=1{where_clause}
            ORDER BY ts DESC LIMIT ${len(values) + 1} OFFSET ${len(values) + 2}
        """
        values.extend([limit, offset])
        data = query_db(data_sql, values, fetch="all")
        visible_columns = params.split(",") if params != "all" else []
        return {
            "data": data,  # List of dicts
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalRecords": total,
            },
            "visibleColumns": visible_columns,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# View: /db (dbview.html with dynamic selectors/data)
@app.get("/db", response_class=HTMLResponse)
def dbview(request: Request):
    try:
        symbols = get_symbols()
        params = get_params()
        data = get_perp_metrics(page=1, limit=100, params="all")
        return templates.TemplateResponse(
            "dbview.html",
            {"request": request, "symbols": symbols, "params": params, "data": data},
        )
    except Exception as e:
        return HTMLResponse(f"<h1>Error: {str(e)}</h1>", status_code=500)


# View: /cards (cardsview.html with latest)
@app.get("/cards", response_class=HTMLResponse)
def cardsview(request: Request):
    try:
        data = get_perp_metrics(page=1, limit=10, params="all")  # Latest for alerts
        return templates.TemplateResponse(
            "cardsview.html", {"request": request, "alerts": data}
        )
    except Exception as e:
        return HTMLResponse(f"<h1>Error: {str(e)}</h1>", status_code=500)


# View: /index (dashboard)
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# Health (uses DBManager)
@app.get("/health")
def health():
    try:
        test = query_db("SELECT 1 as test", fetch="one")
        return (
            {"status": "healthy"}
            if test and test["test"] == 1
            else {"status": "unhealthy"}
        )
    except Exception as e:
        return {"status": "unhealthy", "reason": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3000)
