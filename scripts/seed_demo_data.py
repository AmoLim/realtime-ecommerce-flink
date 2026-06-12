import argparse
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.database import DB_PATH, init_db  # noqa: E402
from flink_jobs.order_stream_job import (  # noqa: E402
    DEVICE_ID,
    EVENT_TIME,
    IP_ADDRESS,
    USER_ID,
    build_order_alert,
    build_shared_identity_alerts,
    build_user_behavior_alerts,
    format_millis,
    has_order_alert,
    parse_event_time_millis,
)
from generator.order_stream_generator import CITIES, PRODUCTS, make_order  # noqa: E402


def reset_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM orders;
        DELETE FROM sales_metrics;
        DELETE FROM product_metrics;
        DELETE FROM city_metrics;
        DELETE FROM alerts;
        """
    )


def order_to_tuple(order: dict) -> tuple:
    return (
        order["order_id"],
        order["user_id"],
        order["product_id"],
        order["product_name"],
        order["category"],
        order["city"],
        order["amount"],
        order["quantity"],
        order["payment_status"],
        order["device_id"],
        order["ip_address"],
        order["event_time"],
    )


def insert_alert(conn: sqlite3.Connection, alert: tuple) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO alerts (
            alert_id, order_id, user_id, alert_type, risk_level, reason, amount, event_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        alert,
    )


def insert_order(conn: sqlite3.Connection, order: dict) -> tuple:
    order_row = order_to_tuple(order)
    conn.execute(
        """
        INSERT OR IGNORE INTO orders (
            order_id, user_id, product_id, product_name, category, city,
            amount, quantity, payment_status, device_id, ip_address, event_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        order_row,
    )

    if has_order_alert(order_row):
        insert_alert(conn, build_order_alert(order_row))

    return order_row


def insert_window_alerts(conn: sqlite3.Connection, orders: list[tuple], window_seconds: int = 10) -> None:
    user_windows: dict[tuple[str, int], list[tuple]] = {}
    ip_windows: dict[tuple[str, int], list[tuple]] = {}
    device_windows: dict[tuple[str, int], list[tuple]] = {}
    window_size_millis = window_seconds * 1000

    for order in orders:
        event_millis = parse_event_time_millis(order[EVENT_TIME])
        window_start = event_millis - event_millis % window_size_millis
        window_end = window_start + window_size_millis
        user_windows.setdefault((order[USER_ID], window_end), []).append(order)
        ip_windows.setdefault((order[IP_ADDRESS], window_end), []).append(order)
        device_windows.setdefault((order[DEVICE_ID], window_end), []).append(order)

    for (user_id, window_end), grouped_orders in user_windows.items():
        window_start = window_end - window_size_millis
        for alert in build_user_behavior_alerts(
            user_id,
            grouped_orders,
            format_millis(window_start),
            format_millis(window_end),
            window_end,
        ):
            insert_alert(conn, alert)

    for (ip_address, window_end), grouped_orders in ip_windows.items():
        window_start = window_end - window_size_millis
        for alert in build_shared_identity_alerts(
            ip_address,
            grouped_orders,
            format_millis(window_start),
            format_millis(window_end),
            window_end,
            threshold=4,
            alert_prefix="SHAREDIP",
            alert_type="SHARED_IP_USERS",
            identity_label="IP",
        ):
            insert_alert(conn, alert)

    for (device_id, window_end), grouped_orders in device_windows.items():
        window_start = window_end - window_size_millis
        for alert in build_shared_identity_alerts(
            device_id,
            grouped_orders,
            format_millis(window_start),
            format_millis(window_end),
            window_end,
            threshold=3,
            alert_prefix="SHAREDDEV",
            alert_type="SHARED_DEVICE_USERS",
            identity_label="设备",
        ):
            insert_alert(conn, alert)


def insert_window_metrics(conn: sqlite3.Connection, windows: int) -> None:
    now = datetime.now().replace(microsecond=0)
    for idx in range(windows):
        window_end = now - timedelta(seconds=(windows - idx - 1) * 10)
        window_start = window_end - timedelta(seconds=10)
        order_count = random.randint(18, 45)
        total_amount = round(random.uniform(12000, 52000), 2)
        start_text = window_start.strftime("%Y-%m-%d %H:%M:%S")
        end_text = window_end.strftime("%Y-%m-%d %H:%M:%S")

        conn.execute(
            """
            INSERT INTO sales_metrics (window_start, window_end, order_count, total_amount)
            VALUES (?, ?, ?, ?)
            """,
            (start_text, end_text, order_count, total_amount),
        )

        for product_id, product_name, _, _, _ in random.sample(PRODUCTS, k=5):
            conn.execute(
                """
                INSERT INTO product_metrics (
                    window_start, window_end, product_id, product_name, order_count, total_amount
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (start_text, end_text, product_id, product_name, random.randint(2, 12), round(random.uniform(1000, 12000), 2)),
            )

        for city in random.sample(CITIES, k=5):
            conn.execute(
                """
                INSERT INTO city_metrics (window_start, window_end, city, order_count, total_amount)
                VALUES (?, ?, ?, ?, ?)
                """,
                (start_text, end_text, city, random.randint(2, 18), round(random.uniform(2000, 16000), 2)),
            )


def seed(rows: int, windows: int, reset: bool) -> None:
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        if reset:
            reset_tables(conn)

        base_time = datetime.now() - timedelta(minutes=5)
        order_rows = []
        for idx in range(1, rows + 1):
            order = make_order(idx)
            order["event_time"] = (base_time + timedelta(seconds=idx * 3)).strftime("%Y-%m-%d %H:%M:%S")
            order_rows.append(insert_order(conn, order))

        insert_window_alerts(conn, order_rows)
        insert_window_metrics(conn, windows)
        conn.commit()

    print(f"Seeded {rows} demo orders and {windows} metric windows into {DB_PATH}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed demo data for frontend preview.")
    parser.add_argument("--rows", type=int, default=120)
    parser.add_argument("--windows", type=int, default=24)
    parser.add_argument("--no-reset", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    seed(args.rows, args.windows, reset=not args.no_reset)
