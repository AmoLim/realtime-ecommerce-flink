import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "ecommerce_monitor.db"

HIGH_AMOUNT_THRESHOLD = 5000
RISKY_DEVICES = {"D-RISK-001", "D-RISK-002", "D-RISK-003"}
RISKY_IPS = {"10.10.8.8", "10.10.9.9", "172.16.66.6"}

ORDER_ID = 0
USER_ID = 1
PRODUCT_ID = 2
PRODUCT_NAME = 3
CATEGORY = 4
CITY = 5
AMOUNT = 6
QUANTITY = 7
PAYMENT_STATUS = 8
DEVICE_ID = 9
IP_ADDRESS = 10
EVENT_TIME = 11


def configure_connection(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")


def ensure_tables(conn):
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


def initialize_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        configure_connection(conn)
        ensure_tables(conn)
        conn.commit()


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    configure_connection(conn)
    return conn


def parse_order(line):
    item = json.loads(line)
    return (
        str(item["order_id"]),
        str(item["user_id"]),
        str(item["product_id"]),
        str(item["product_name"]),
        str(item["category"]),
        str(item["city"]),
        float(item["amount"]),
        int(item["quantity"]),
        str(item["payment_status"]),
        str(item["device_id"]),
        str(item["ip_address"]),
        str(item["event_time"]),
    )


def is_json_line(line):
    text = line.strip()
    return text.startswith("{") and text.endswith("}")


def format_millis(epoch_millis):
    return datetime.fromtimestamp(epoch_millis / 1000).strftime("%Y-%m-%d %H:%M:%S")


def create_socket_text_stream(env, host, port, type_info):
    if hasattr(env, "socket_text_stream"):
        return env.socket_text_stream(host, port)

    from pyflink.datastream.functions import SourceFunction
    from pyflink.java_gateway import get_gateway

    gateway = get_gateway()
    source_class_names = [
        "org.apache.flink.streaming.api.functions.source.legacy.SocketTextStreamFunction",
        "org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction",
    ]
    last_error = None
    for class_name in source_class_names:
        try:
            source_class = gateway.jvm.__getattr__(class_name)
            java_source = source_class(host, int(port), "\n", 0)
            return env.add_source(SourceFunction(java_source), source_name="socket-order-source", type_info=type_info)
        except Exception as exc:  # pragma: no cover - depends on the installed Flink version.
            last_error = exc

    raise RuntimeError("Could not create a socket text source for this PyFlink version.") from last_error


def save_order(order):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO orders (
                order_id, user_id, product_id, product_name, category, city,
                amount, quantity, payment_status, device_id, ip_address, event_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            order,
        )
    return f"order saved: {order[ORDER_ID]}"


def has_order_alert(order):
    return (
        float(order[AMOUNT]) >= HIGH_AMOUNT_THRESHOLD
        or order[PAYMENT_STATUS] == "FAILED"
        or order[DEVICE_ID] in RISKY_DEVICES
        or order[IP_ADDRESS] in RISKY_IPS
    )


def build_order_alert(order):
    alert_types = []
    reasons = []
    risk_level = "MEDIUM"
    amount = float(order[AMOUNT])

    if amount >= HIGH_AMOUNT_THRESHOLD:
        alert_types.append("HIGH_AMOUNT")
        reasons.append(f"单笔订单金额 {amount:.2f} 元，超过 {HIGH_AMOUNT_THRESHOLD} 元阈值")
        risk_level = "HIGH"
    if order[PAYMENT_STATUS] == "FAILED":
        alert_types.append("PAYMENT_FAILED")
        reasons.append("订单支付状态为 FAILED")
    if order[DEVICE_ID] in RISKY_DEVICES:
        alert_types.append("RISKY_DEVICE")
        reasons.append(f"命中风险设备 {order[DEVICE_ID]}")
        risk_level = "HIGH"
    if order[IP_ADDRESS] in RISKY_IPS:
        alert_types.append("RISKY_IP")
        reasons.append(f"命中风险 IP {order[IP_ADDRESS]}")
        risk_level = "HIGH"

    alert_type = "+".join(alert_types)
    alert_id = f"A-{order[ORDER_ID]}-{alert_type}".replace("+", "-")
    return (
        alert_id,
        order[ORDER_ID],
        order[USER_ID],
        alert_type,
        risk_level,
        "；".join(reasons),
        amount,
        order[EVENT_TIME],
    )


def save_order_alert(order):
    return save_alert_record(build_order_alert(order))


def save_alert_record(alert):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO alerts (
                alert_id, order_id, user_id, alert_type, risk_level, reason, amount, event_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            alert,
        )
    return f"alert saved: {alert[3]} {alert[1]}"


def save_sales_metric(metric):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sales_metrics (window_start, window_end, order_count, total_amount)
            VALUES (?, ?, ?, ?)
            """,
            (metric[0], metric[1], int(metric[2]), round(float(metric[3]), 2)),
        )
    return f"sales window saved: {metric[0]} - {metric[1]}"


def save_product_metric(metric):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO product_metrics (
                window_start, window_end, product_id, product_name, order_count, total_amount
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (metric[0], metric[1], metric[2], metric[3], int(metric[4]), round(float(metric[5]), 2)),
        )
    return f"product window saved: {metric[3]}"


def save_city_metric(metric):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO city_metrics (window_start, window_end, city, order_count, total_amount)
            VALUES (?, ?, ?, ?, ?)
            """,
            (metric[0], metric[1], metric[2], int(metric[3]), round(float(metric[4]), 2)),
        )
    return f"city window saved: {metric[2]}"


def parse_args():
    parser = argparse.ArgumentParser(description="PyFlink realtime ecommerce order monitor.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--window-seconds", type=int, default=10)
    parser.add_argument("--frequency-threshold", type=int, default=6)
    return parser.parse_args()


def main():
    try:
        from pyflink.common import Types
        from pyflink.common.time import Time
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.functions import ProcessWindowFunction
        from pyflink.datastream.window import TumblingProcessingTimeWindows
    except ImportError as exc:
        print("PyFlink is not installed. Activate your conda env or run: pip install -r requirements-flink.txt", file=sys.stderr)
        raise exc

    class SalesWindowFunction(ProcessWindowFunction):
        def process(self, key, context, elements):
            order_count = 0
            total_amount = 0.0
            for order in elements:
                order_count += 1
                total_amount += float(order[AMOUNT])
            if order_count:
                window = context.window()
                yield (format_millis(window.start), format_millis(window.end), order_count, round(total_amount, 2))

    class ProductWindowFunction(ProcessWindowFunction):
        def process(self, key, context, elements):
            order_count = 0
            total_amount = 0.0
            product_name = ""
            for order in elements:
                order_count += 1
                total_amount += float(order[AMOUNT])
                product_name = order[PRODUCT_NAME]
            if order_count:
                window = context.window()
                yield (
                    format_millis(window.start),
                    format_millis(window.end),
                    key,
                    product_name,
                    order_count,
                    round(total_amount, 2),
                )

    class CityWindowFunction(ProcessWindowFunction):
        def process(self, key, context, elements):
            order_count = 0
            total_amount = 0.0
            for order in elements:
                order_count += 1
                total_amount += float(order[AMOUNT])
            if order_count:
                window = context.window()
                yield (format_millis(window.start), format_millis(window.end), key, order_count, round(total_amount, 2))

    class UserFrequencyWindowFunction(ProcessWindowFunction):
        def __init__(self, threshold):
            self.threshold = threshold

        def process(self, key, context, elements):
            order_count = 0
            total_amount = 0.0
            latest_event_time = ""
            latest_order_id = ""
            for order in elements:
                order_count += 1
                total_amount += float(order[AMOUNT])
                latest_event_time = order[EVENT_TIME]
                latest_order_id = order[ORDER_ID]

            if order_count >= self.threshold:
                window = context.window()
                window_end = format_millis(window.end)
                risk_level = "HIGH" if order_count >= self.threshold * 2 else "MEDIUM"
                alert_id = f"A-FREQ-{key}-{window.end}"
                reason = f"用户在 {format_millis(window.start)} 至 {window_end} 内下单 {order_count} 次，达到高频阈值 {self.threshold}"
                yield (
                    alert_id,
                    latest_order_id or f"WINDOW-{key}-{window.end}",
                    key,
                    "FREQUENT_USER_ORDERS",
                    risk_level,
                    reason,
                    round(total_amount, 2),
                    latest_event_time or window_end,
                )

    args = parse_args()
    initialize_database()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)

    order_type = Types.TUPLE(
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]
    )
    sales_metric_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE()])
    product_metric_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE()])
    city_metric_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE()])
    alert_type = Types.TUPLE(
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.STRING(),
        ]
    )

    orders = (
        create_socket_text_stream(env, args.host, args.port, Types.STRING())
        .filter(is_json_line)
        .map(parse_order, output_type=order_type)
    )

    orders.map(save_order, output_type=Types.STRING()).print()
    orders.filter(has_order_alert).map(save_order_alert, output_type=Types.STRING()).print()

    window_assigner = TumblingProcessingTimeWindows.of(Time.seconds(args.window_seconds))

    (
        orders.key_by(lambda order: "sales")
        .window(window_assigner)
        .process(SalesWindowFunction(), output_type=sales_metric_type)
        .map(save_sales_metric, output_type=Types.STRING())
        .print()
    )

    (
        orders.key_by(lambda order: order[PRODUCT_ID])
        .window(window_assigner)
        .process(ProductWindowFunction(), output_type=product_metric_type)
        .map(save_product_metric, output_type=Types.STRING())
        .print()
    )

    (
        orders.key_by(lambda order: order[CITY])
        .window(window_assigner)
        .process(CityWindowFunction(), output_type=city_metric_type)
        .map(save_city_metric, output_type=Types.STRING())
        .print()
    )

    (
        orders.key_by(lambda order: order[USER_ID])
        .window(window_assigner)
        .process(UserFrequencyWindowFunction(args.frequency_threshold), output_type=alert_type)
        .map(save_alert_record, output_type=Types.STRING())
        .print()
    )

    env.execute("realtime-ecommerce-order-monitor")


if __name__ == "__main__":
    main()
