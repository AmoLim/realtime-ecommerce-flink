import argparse
import json
import random
import socket
import time
import warnings
from datetime import datetime, timedelta


PRODUCTS = [
    ("P1001", "无线耳机", "数码", 399, 1299),
    ("P1002", "机械键盘", "数码", 199, 899),
    ("P1003", "运动手环", "数码", 129, 599),
    ("P2001", "羽绒服", "服饰", 299, 1299),
    ("P2002", "跑步鞋", "服饰", 199, 999),
    ("P3001", "咖啡豆", "食品", 39, 199),
    ("P3002", "坚果礼盒", "食品", 49, 399),
    ("P4001", "人体工学椅", "家居", 599, 2999),
]

CITIES = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"]
PAYMENT_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]
RISKY_DEVICES = ["D-RISK-001", "D-RISK-002", "D-RISK-003"]
RISKY_IPS = ["10.10.8.8", "10.10.9.9", "172.16.66.6"]
BURST_USERS = ["U-BURST-001", "U-BURST-002", "U-BURST-003"]
FAILED_USERS = ["U-FAIL-001", "U-FAIL-002"]
DEVICE_HOP_USER = "U-DEVICE-HOP-001"
GEO_HOP_USER = "U-GEO-HOP-001"
SHARED_IP_USERS = ["U-SHARED-IP-001", "U-SHARED-IP-002", "U-SHARED-IP-003", "U-SHARED-IP-004", "U-SHARED-IP-005"]
SHARED_DEVICE_USERS = ["U-SHARED-DEV-001", "U-SHARED-DEV-002", "U-SHARED-DEV-003", "U-SHARED-DEV-004"]
DEVICE_HOP_DEVICES = ["D-HOP-001", "D-HOP-002", "D-HOP-003", "D-HOP-004", "D-HOP-005"]
SHARED_IP_POOL = ["172.20.88.8", "172.20.88.9"]
SHARED_DEVICE_POOL = ["D-SHARED-001", "D-SHARED-002"]


def make_order(sequence: int) -> dict:
    product_id, product_name, category, min_price, max_price = random.choice(PRODUCTS)
    quantity = random.choices([1, 2, 3, 4], weights=[72, 18, 7, 3], k=1)[0]
    amount = round(random.uniform(min_price, max_price) * quantity, 2)
    anomaly_roll = random.random()
    scenario = "NORMAL"

    payment_status = random.choice(PAYMENT_STATUSES)
    if anomaly_roll < 0.05:
        payment_status = "FAILED"
        scenario = "PAYMENT_FAILED"

    user_id = f"U{random.randint(1000, 1999)}"
    if 0.05 <= anomaly_roll < 0.12:
        user_id = random.choice(BURST_USERS)
        scenario = "BURST_USER"
    if 8 <= sequence % 45 <= 15:
        user_id = BURST_USERS[(sequence // 45) % len(BURST_USERS)]
        scenario = "BURST_USER"

    device_id = f"D{random.randint(1000, 9999)}"
    ip_address = f"192.168.{random.randint(1, 40)}.{random.randint(2, 250)}"
    city = random.choice(CITIES)

    if 0.12 <= anomaly_roll < 0.18:
        device_id = random.choice(RISKY_DEVICES)
        ip_address = random.choice(RISKY_IPS)
        scenario = "RISKY_DEVICE_IP"

    phase = sequence % 60
    if 18 <= phase <= 22:
        user_id = FAILED_USERS[(sequence // 60) % len(FAILED_USERS)]
        payment_status = "FAILED"
        scenario = "REPEATED_PAYMENT_FAILURES"
    elif 24 <= phase <= 28:
        user_id = DEVICE_HOP_USER
        device_id = DEVICE_HOP_DEVICES[phase - 24]
        scenario = "DEVICE_HOPPING"
    elif 30 <= phase <= 34:
        user_id = GEO_HOP_USER
        city = CITIES[(phase - 30) % len(CITIES)]
        scenario = "GEO_VELOCITY"
    elif 36 <= phase <= 42:
        user_id = SHARED_IP_USERS[(phase - 36) % len(SHARED_IP_USERS)]
        ip_address = SHARED_IP_POOL[(sequence // 60) % len(SHARED_IP_POOL)]
        scenario = "SHARED_IP_USERS"
    elif 44 <= phase <= 49:
        user_id = SHARED_DEVICE_USERS[(phase - 44) % len(SHARED_DEVICE_USERS)]
        device_id = SHARED_DEVICE_POOL[(sequence // 60) % len(SHARED_DEVICE_POOL)]
        scenario = "SHARED_DEVICE_USERS"

    event_time = datetime.now()
    if sequence % 37 == 0:
        event_time -= timedelta(seconds=random.randint(1, 3))

    return {
        "order_id": f"O{datetime.now().strftime('%Y%m%d%H%M%S')}{sequence:06d}",
        "user_id": user_id,
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "city": city,
        "amount": amount,
        "quantity": quantity,
        "payment_status": payment_status,
        "device_id": device_id,
        "ip_address": ip_address,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "scenario": scenario,
    }


def serve(host: str, port: int, interval: float, total: int, quiet: bool) -> None:
    sequence = 1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)
        print(f"Order stream generator listening on {host}:{port}")

        while total <= 0 or sequence <= total:
            conn, addr = server.accept()
            print(f"Client connected: {addr}")
            with conn:
                while total <= 0 or sequence <= total:
                    line = json.dumps(make_order(sequence), ensure_ascii=False) + "\n"
                    try:
                        conn.sendall(line.encode("utf-8"))
                    except BrokenPipeError:
                        print("Client disconnected. Waiting for a new connection.")
                        break
                    if not quiet:
                        print(line.strip())
                    sequence += 1
                    time.sleep(interval)


def produce_to_kafka(bootstrap_servers: str, topic: str, interval: float, total: int, quiet: bool) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Kafka producer requires kafka-python. Run: pip install -r requirements.txt") from exc

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="value_serializer does not implement kafka.serializer.Serializer")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=10,
            max_block_ms=10000,
            request_timeout_ms=10000,
        )
    print(f"Order stream generator publishing to Kafka topic {topic} at {bootstrap_servers}")

    sequence = 1
    while total <= 0 or sequence <= total:
        order = make_order(sequence)
        try:
            producer.send(topic, value=order).get(timeout=10)
        except Exception as exc:
            producer.close()
            raise RuntimeError(
                f"Could not publish to Kafka topic {topic} at {bootstrap_servers}. "
                "Check that Kafka is running and the topic exists."
            ) from exc
        if not quiet:
            print(json.dumps(order, ensure_ascii=False))
        sequence += 1
        if sequence % 20 == 0:
            producer.flush()
        time.sleep(interval)

    producer.flush()
    producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate realtime ecommerce order stream.")
    parser.add_argument("--sink", choices=["socket", "kafka"], default="socket", help="Where generated orders are sent.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--kafka-bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--kafka-topic", default="ecommerce-orders")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between orders.")
    parser.add_argument("--total", type=int, default=0, help="0 means infinite stream.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatable demos.")
    parser.add_argument("--quiet", action="store_true", help="Do not print every generated order.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    if args.sink == "kafka":
        produce_to_kafka(args.kafka_bootstrap_servers, args.kafka_topic, args.interval, args.total, args.quiet)
    else:
        serve(args.host, args.port, args.interval, args.total, args.quiet)
