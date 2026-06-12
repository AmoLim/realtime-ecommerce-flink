from typing import Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from .database import DB_PATH, PROJECT_ROOT, get_connection, init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Realtime Ecommerce Monitor", version="0.2.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fetch_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]


def fetch_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "database": str(DB_PATH)}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    dashboard_file = PROJECT_ROOT / "frontend" / "standalone.html"
    return dashboard_file.read_text(encoding="utf-8")


@app.get("/api/metrics/summary")
def get_summary() -> dict[str, Any]:
    order_stats = fetch_one(
        """
        SELECT COUNT(*) AS total_orders, COALESCE(SUM(amount), 0) AS total_sales
        FROM orders
        """
    )
    alert_stats = fetch_one("SELECT COUNT(*) AS alert_count FROM alerts")
    latest_window = fetch_one(
        """
        SELECT order_count, total_amount, window_end
        FROM sales_metrics
        ORDER BY window_end DESC, id DESC
        LIMIT 1
        """
    )

    return {
        "total_orders": order_stats["total_orders"] if order_stats else 0,
        "total_sales": round(order_stats["total_sales"] if order_stats else 0, 2),
        "alert_count": alert_stats["alert_count"] if alert_stats else 0,
        "latest_window_orders": latest_window["order_count"] if latest_window else 0,
        "latest_window_sales": round(latest_window["total_amount"] if latest_window else 0, 2),
        "latest_window_end": latest_window["window_end"] if latest_window else None,
    }


@app.get("/api/metrics/sales-trend")
def get_sales_trend(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
            window_end AS time,
            SUM(order_count) AS order_count,
            ROUND(SUM(total_amount), 2) AS total_amount
        FROM sales_metrics
        GROUP BY window_end
        ORDER BY window_end DESC
        LIMIT ?
        """,
        (limit,),
    )
    return list(reversed(rows))


@app.get("/api/metrics/top-products")
def get_top_products(limit: int = Query(default=8, ge=1, le=50)) -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        WITH recent_windows AS (
            SELECT DISTINCT window_end
            FROM product_metrics
            ORDER BY window_end DESC
            LIMIT 12
        )
        SELECT
            product_id,
            product_name,
            SUM(order_count) AS order_count,
            ROUND(SUM(total_amount), 2) AS total_amount
        FROM product_metrics
        WHERE window_end IN (SELECT window_end FROM recent_windows)
        GROUP BY product_id, product_name
        ORDER BY total_amount DESC
        LIMIT ?
        """,
        (limit,),
    )
    if rows:
        return rows

    return fetch_all(
        """
        SELECT
            product_id,
            product_name,
            COUNT(*) AS order_count,
            ROUND(SUM(amount), 2) AS total_amount
        FROM orders
        GROUP BY product_id, product_name
        ORDER BY total_amount DESC
        LIMIT ?
        """,
        (limit,),
    )


@app.get("/api/metrics/city-distribution")
def get_city_distribution() -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        WITH recent_windows AS (
            SELECT DISTINCT window_end
            FROM city_metrics
            ORDER BY window_end DESC
            LIMIT 12
        )
        SELECT
            city,
            SUM(order_count) AS order_count,
            ROUND(SUM(total_amount), 2) AS total_amount
        FROM city_metrics
        WHERE window_end IN (SELECT window_end FROM recent_windows)
        GROUP BY city
        ORDER BY total_amount DESC
        """
    )
    if rows:
        return rows

    return fetch_all(
        """
        SELECT city, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS total_amount
        FROM orders
        GROUP BY city
        ORDER BY total_amount DESC
        """
    )


@app.get("/api/orders/latest")
def get_latest_orders(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            order_id,
            user_id,
            product_name,
            city,
            amount,
            payment_status,
            event_time
        FROM orders
        ORDER BY event_time DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )


@app.get("/api/alerts")
def get_alerts(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            alert_id,
            order_id,
            user_id,
            alert_type,
            risk_level,
            reason,
            amount,
            event_time
        FROM alerts
        ORDER BY event_time DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
