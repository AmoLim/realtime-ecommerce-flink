import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "ecommerce_monitor.db"
DEFAULT_KAFKA_CONNECTOR_JAR = PROJECT_ROOT / "lib" / "flink-sql-connector-kafka-4.0.0-2.0.jar"

RISKY_DEVICES = {"D-RISK-001", "D-RISK-002", "D-RISK-003"}
RISKY_IPS = {"10.10.8.8", "10.10.9.9", "172.16.66.6"}

DEFAULT_FREQUENCY_THRESHOLD = 6
DEFAULT_FAILED_PAYMENT_THRESHOLD = 3
DEFAULT_DEVICE_HOP_THRESHOLD = 3
DEFAULT_CITY_HOP_THRESHOLD = 3
DEFAULT_SHARED_IP_USER_THRESHOLD = 4
DEFAULT_SHARED_DEVICE_USER_THRESHOLD = 3

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


def parse_event_time_millis(event_time):
    return int(datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


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


def create_kafka_text_stream(env, bootstrap_servers, topic, group_id, offset, type_info):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import WatermarkStrategy
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    offset_initializers = {
        "earliest": KafkaOffsetsInitializer.earliest,
        "latest": KafkaOffsetsInitializer.latest,
    }
    try:
        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(bootstrap_servers)
            .set_topics(topic)
            .set_group_id(group_id)
            .set_starting_offsets(offset_initializers[offset]())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    except Exception as exc:
        message = str(exc)
        if (
            "Could not found the Java class" in message
            or "NoClassDefFoundError" in message
            or "ClassNotFoundException" in message
        ):
            raise RuntimeError(
                "Flink Kafka connector JAR is missing or incomplete in the PyFlink Java classpath. "
                "Use the fat SQL connector JAR, not the thin flink-connector-kafka JAR. "
                "Run: python -m pip install -r requirements-flink.txt && "
                "scripts/install_flink_kafka_connector.sh, then restart this job. "
                f"Default JAR path: {DEFAULT_KAFKA_CONNECTOR_JAR}"
            ) from exc
        raise
    return env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-order-source", type_info=type_info)


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
    return order[PAYMENT_STATUS] == "FAILED" or order[DEVICE_ID] in RISKY_DEVICES or order[IP_ADDRESS] in RISKY_IPS


def build_order_alert(order):
    alert_types = []
    reasons = []
    risk_level = "MEDIUM"
    amount = float(order[AMOUNT])

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


def summarize_orders(orders):
    order_count = len(orders)
    total_amount = round(sum(float(order[AMOUNT]) for order in orders), 2)
    latest_order = max(orders, key=lambda order: order[EVENT_TIME]) if orders else None
    return order_count, total_amount, latest_order


def window_alert(alert_id, order_id, user_id, alert_type, risk_level, reason, amount, event_time):
    return (alert_id, order_id, user_id, alert_type, risk_level, reason, round(float(amount), 2), event_time)


def build_user_behavior_alerts(
    user_id,
    orders,
    window_start,
    window_end,
    window_end_millis,
    frequency_threshold=DEFAULT_FREQUENCY_THRESHOLD,
    failed_payment_threshold=DEFAULT_FAILED_PAYMENT_THRESHOLD,
    device_hop_threshold=DEFAULT_DEVICE_HOP_THRESHOLD,
    city_hop_threshold=DEFAULT_CITY_HOP_THRESHOLD,
):
    order_count, total_amount, latest_order = summarize_orders(orders)
    if not order_count:
        return []

    latest_order_id = latest_order[ORDER_ID]
    latest_event_time = latest_order[EVENT_TIME]
    failed_count = sum(1 for order in orders if order[PAYMENT_STATUS] == "FAILED")
    devices = {order[DEVICE_ID] for order in orders}
    cities = {order[CITY] for order in orders}
    alerts = []

    if order_count >= frequency_threshold:
        risk_level = "HIGH" if order_count >= frequency_threshold * 2 else "MEDIUM"
        reason = f"用户在 {window_start} 至 {window_end} 内下单 {order_count} 次，达到高频阈值 {frequency_threshold}"
        alerts.append(
            window_alert(
                f"A-FREQ-{user_id}-{window_end_millis}",
                latest_order_id,
                user_id,
                "FREQUENT_USER_ORDERS",
                risk_level,
                reason,
                total_amount,
                latest_event_time,
            )
        )

    if failed_count >= failed_payment_threshold:
        risk_level = "HIGH" if failed_count == order_count or failed_count >= failed_payment_threshold * 2 else "MEDIUM"
        reason = f"用户在 {window_start} 至 {window_end} 内支付失败 {failed_count} 次，达到失败支付阈值 {failed_payment_threshold}"
        alerts.append(
            window_alert(
                f"A-FAIL-{user_id}-{window_end_millis}",
                latest_order_id,
                user_id,
                "REPEATED_PAYMENT_FAILURES",
                risk_level,
                reason,
                total_amount,
                latest_event_time,
            )
        )

    if len(devices) >= device_hop_threshold:
        risk_level = "HIGH" if len(devices) >= device_hop_threshold + 2 else "MEDIUM"
        reason = f"用户在 {window_start} 至 {window_end} 内切换 {len(devices)} 个设备，达到设备跳变阈值 {device_hop_threshold}"
        alerts.append(
            window_alert(
                f"A-DEVHOP-{user_id}-{window_end_millis}",
                latest_order_id,
                user_id,
                "DEVICE_HOPPING",
                risk_level,
                reason,
                total_amount,
                latest_event_time,
            )
        )

    if len(cities) >= city_hop_threshold:
        risk_level = "HIGH" if len(cities) >= city_hop_threshold + 2 else "MEDIUM"
        reason = f"用户在 {window_start} 至 {window_end} 内跨 {len(cities)} 个城市下单，达到异地跳变阈值 {city_hop_threshold}"
        alerts.append(
            window_alert(
                f"A-GEO-{user_id}-{window_end_millis}",
                latest_order_id,
                user_id,
                "GEO_VELOCITY",
                risk_level,
                reason,
                total_amount,
                latest_event_time,
            )
        )

    return alerts


def build_shared_identity_alerts(
    identity_value,
    orders,
    window_start,
    window_end,
    window_end_millis,
    threshold,
    alert_prefix,
    alert_type,
    identity_label,
):
    order_count, total_amount, latest_order = summarize_orders(orders)
    if not order_count:
        return []

    users = {order[USER_ID] for order in orders}
    if len(users) < threshold:
        return []

    risk_level = "HIGH" if len(users) >= threshold + 2 else "MEDIUM"
    latest_order_id = latest_order[ORDER_ID]
    latest_event_time = latest_order[EVENT_TIME]
    reason = f"同一{identity_label} {identity_value} 在 {window_start} 至 {window_end} 内关联 {len(users)} 个用户，达到共享身份阈值 {threshold}"
    return [
        window_alert(
            f"A-{alert_prefix}-{identity_value}-{window_end_millis}",
            latest_order_id,
            ",".join(sorted(users)),
            alert_type,
            risk_level,
            reason,
            total_amount,
            latest_event_time,
        )
    ]


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
    parser.add_argument("--source", choices=["socket", "kafka"], default="socket", help="Realtime input source.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--kafka-bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--kafka-topic", default="ecommerce-orders")
    parser.add_argument("--kafka-group-id", default="ecommerce-monitor")
    parser.add_argument("--kafka-offset", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--kafka-connector-jar", default="", help="Optional local Flink Kafka connector JAR path.")
    parser.add_argument("--window-seconds", type=int, default=10)
    parser.add_argument("--watermark-delay-seconds", type=int, default=3)
    parser.add_argument("--frequency-threshold", type=int, default=DEFAULT_FREQUENCY_THRESHOLD)
    parser.add_argument("--failed-payment-threshold", type=int, default=DEFAULT_FAILED_PAYMENT_THRESHOLD)
    parser.add_argument("--device-hop-threshold", type=int, default=DEFAULT_DEVICE_HOP_THRESHOLD)
    parser.add_argument("--city-hop-threshold", type=int, default=DEFAULT_CITY_HOP_THRESHOLD)
    parser.add_argument("--shared-ip-user-threshold", type=int, default=DEFAULT_SHARED_IP_USER_THRESHOLD)
    parser.add_argument("--shared-device-user-threshold", type=int, default=DEFAULT_SHARED_DEVICE_USER_THRESHOLD)
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        from pyflink.common import Types
        from pyflink.common.time import Duration
        from pyflink.common.time import Time
        from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.functions import ProcessWindowFunction
        from pyflink.datastream.window import TumblingEventTimeWindows
    except ImportError as exc:
        print("PyFlink is not installed. Activate your conda env or run: pip install -r requirements-flink.txt", file=sys.stderr)
        raise exc

    class OrderEventTimeAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return parse_event_time_millis(value[EVENT_TIME])

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

    class UserBehaviorWindowFunction(ProcessWindowFunction):
        def __init__(self, frequency_threshold, failed_payment_threshold, device_hop_threshold, city_hop_threshold):
            self.frequency_threshold = frequency_threshold
            self.failed_payment_threshold = failed_payment_threshold
            self.device_hop_threshold = device_hop_threshold
            self.city_hop_threshold = city_hop_threshold

        def process(self, key, context, elements):
            window = context.window()
            orders = list(elements)
            yield from build_user_behavior_alerts(
                key,
                orders,
                format_millis(window.start),
                format_millis(window.end),
                window.end,
                self.frequency_threshold,
                self.failed_payment_threshold,
                self.device_hop_threshold,
                self.city_hop_threshold,
            )

    class SharedIdentityWindowFunction(ProcessWindowFunction):
        def __init__(self, threshold, alert_prefix, alert_type, identity_label):
            self.threshold = threshold
            self.alert_prefix = alert_prefix
            self.alert_type = alert_type
            self.identity_label = identity_label

        def process(self, key, context, elements):
            window = context.window()
            orders = list(elements)
            yield from build_shared_identity_alerts(
                key,
                orders,
                format_millis(window.start),
                format_millis(window.end),
                window.end,
                self.threshold,
                self.alert_prefix,
                self.alert_type,
                self.identity_label,
            )

    initialize_database()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)
    env.get_config().set_auto_watermark_interval(1000)
    kafka_connector_jar = Path(args.kafka_connector_jar).resolve() if args.kafka_connector_jar else DEFAULT_KAFKA_CONNECTOR_JAR
    if args.source == "kafka" and kafka_connector_jar.exists():
        env.add_jars(kafka_connector_jar.as_uri())

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

    if args.source == "kafka":
        raw_orders = create_kafka_text_stream(
            env,
            args.kafka_bootstrap_servers,
            args.kafka_topic,
            args.kafka_group_id,
            args.kafka_offset,
            Types.STRING(),
        )
    else:
        raw_orders = create_socket_text_stream(env, args.host, args.port, Types.STRING())

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(args.watermark_delay_seconds)
    ).with_timestamp_assigner(OrderEventTimeAssigner())
    orders = raw_orders.filter(is_json_line).map(parse_order, output_type=order_type).assign_timestamps_and_watermarks(
        watermark_strategy
    )

    orders.map(save_order, output_type=Types.STRING()).print()
    orders.filter(has_order_alert).map(save_order_alert, output_type=Types.STRING()).print()

    window_assigner = TumblingEventTimeWindows.of(Time.seconds(args.window_seconds))

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
        .process(
            UserBehaviorWindowFunction(
                args.frequency_threshold,
                args.failed_payment_threshold,
                args.device_hop_threshold,
                args.city_hop_threshold,
            ),
            output_type=alert_type,
        )
        .map(save_alert_record, output_type=Types.STRING())
        .print()
    )

    (
        orders.key_by(lambda order: order[IP_ADDRESS])
        .window(window_assigner)
        .process(
            SharedIdentityWindowFunction(
                args.shared_ip_user_threshold,
                "SHAREDIP",
                "SHARED_IP_USERS",
                "IP",
            ),
            output_type=alert_type,
        )
        .map(save_alert_record, output_type=Types.STRING())
        .print()
    )

    (
        orders.key_by(lambda order: order[DEVICE_ID])
        .window(window_assigner)
        .process(
            SharedIdentityWindowFunction(
                args.shared_device_user_threshold,
                "SHAREDDEV",
                "SHARED_DEVICE_USERS",
                "设备",
            ),
            output_type=alert_type,
        )
        .map(save_alert_record, output_type=Types.STRING())
        .print()
    )

    env.execute("realtime-ecommerce-order-monitor")


if __name__ == "__main__":
    main()
