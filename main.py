import os
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from psycopg2.extensions import connection as PsycopgConnection

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "1q2w3er4")
    DB_HOST = os.getenv("DB_HOST", "db")  # 'db' for Docker service name
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "fademoe5")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@contextmanager
def get_db() -> Generator[Optional[PsycopgConnection], None, None]:
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"DB connection failed: {e}")
    try:
        yield conn
    finally:
        if conn is not None:
            conn.close()


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


@app.get("/cards")
def cards():
    return HTMLResponse("""
    <html>
    <head>
        <title>Alert Cards View</title>
        <style>
            body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; padding: 20px; }
            .card { border: 1px solid #888; border-radius: 8px; padding: 10px; margin: 10px 0; background: #2a2a40; }
        </style>
    </head>
    <body>
        <h1>Alert Cards (Embed Placeholder)</h1>
        <div class="card">
            <h3>Sample Alert</h3>
            <p>Time: Now | Symbol: BTC | Signal: Buy</p>
        </div>
        <!-- Load real cardsview.html or /api/alerts data here -->
    </body>
    </html>
    """)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
