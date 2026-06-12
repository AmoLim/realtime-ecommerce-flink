import argparse
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.database import DB_PATH, init_db  # noqa: E402
from generator.order_stream_generator import CITIES, PRODUCTS, RISKY_DEVICES, RISKY_IPS, make_order  # noqa: E402


HIGH_AMOUNT_THRESHOLD = 5000


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


def insert_order(conn: sqlite3.Connection, order: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO orders (
            order_id, user_id, product_id, product_name, category, city,
            amount, quantity, payment_status, device_id, ip_address, event_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
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
        ),
    )

    alert_types = []
    reasons = []
    risk_level = "MEDIUM"

    if order["amount"] >= HIGH_AMOUNT_THRESHOLD:
        alert_types.append("HIGH_AMOUNT")
        reasons.append(f"单笔订单金额 {order['amount']:.2f} 元，超过 {HIGH_AMOUNT_THRESHOLD} 元阈值")
        risk_level = "HIGH"
    if order["payment_status"] == "FAILED":
        alert_types.append("PAYMENT_FAILED")
        reasons.append("订单支付状态为 FAILED")
    if order["device_id"] in RISKY_DEVICES:
        alert_types.append("RISKY_DEVICE")
        reasons.append(f"命中风险设备 {order['device_id']}")
        risk_level = "HIGH"
    if order["ip_address"] in RISKY_IPS:
        alert_types.append("RISKY_IP")
        reasons.append(f"命中风险 IP {order['ip_address']}")
        risk_level = "HIGH"

    if alert_types:
        alert_type = "+".join(alert_types)

        conn.execute(
            """
            INSERT OR IGNORE INTO alerts (
                alert_id, order_id, user_id, alert_type, risk_level, reason, amount, event_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"A-{order['order_id']}-{alert_type}".replace("+", "-"),
                order["order_id"],
                order["user_id"],
                alert_type,
                risk_level,
                "；".join(reasons),
                order["amount"],
                order["event_time"],
            ),
        )


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
        for idx in range(1, rows + 1):
            order = make_order(idx)
            order["event_time"] = (base_time + timedelta(seconds=idx * 3)).strftime("%Y-%m-%d %H:%M:%S")
            insert_order(conn, order)

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
