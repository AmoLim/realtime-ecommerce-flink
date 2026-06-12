from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "ecommerce_monitor.db"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                city TEXT NOT NULL,
                amount REAL NOT NULL,
                quantity INTEGER NOT NULL,
                payment_status TEXT NOT NULL,
                device_id TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                event_time TEXT NOT NULL,
                ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sales_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                order_count INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS product_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                order_count INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS city_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                city TEXT NOT NULL,
                order_count INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id TEXT NOT NULL UNIQUE,
                order_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                reason TEXT NOT NULL,
                amount REAL NOT NULL,
                event_time TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_orders_event_time ON orders(event_time);
            CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id, product_name);
            CREATE INDEX IF NOT EXISTS idx_orders_city ON orders(city);
            CREATE INDEX IF NOT EXISTS idx_sales_window_end ON sales_metrics(window_end);
            CREATE INDEX IF NOT EXISTS idx_product_window_end ON product_metrics(window_end);
            CREATE INDEX IF NOT EXISTS idx_city_window_end ON city_metrics(window_end);
            CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts(event_time);
            CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type, risk_level);
            """
        )
