import argparse
import json
import random
import socket
import time
from datetime import datetime


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


def make_order(sequence: int) -> dict:
    product_id, product_name, category, min_price, max_price = random.choice(PRODUCTS)
    quantity = random.choices([1, 2, 3, 4], weights=[72, 18, 7, 3], k=1)[0]
    amount = round(random.uniform(min_price, max_price) * quantity, 2)
    anomaly_roll = random.random()
    scenario = "NORMAL"

    if anomaly_roll < 0.05:
        amount = round(random.uniform(5000, 15000), 2)
        scenario = "HIGH_AMOUNT"

    payment_status = random.choice(PAYMENT_STATUSES)
    if 0.05 <= anomaly_roll < 0.09:
        payment_status = "FAILED"
        scenario = "PAYMENT_FAILED"

    user_id = f"U{random.randint(1000, 1999)}"
    if 0.09 <= anomaly_roll < 0.16:
        user_id = random.choice(BURST_USERS)
        scenario = "BURST_USER"
    if 8 <= sequence % 45 <= 15:
        user_id = BURST_USERS[(sequence // 45) % len(BURST_USERS)]
        scenario = "BURST_USER"

    device_id = f"D{random.randint(1000, 9999)}"
    ip_address = f"192.168.{random.randint(1, 40)}.{random.randint(2, 250)}"
    if 0.16 <= anomaly_roll < 0.22:
        device_id = random.choice(RISKY_DEVICES)
        ip_address = random.choice(RISKY_IPS)
        scenario = "RISKY_DEVICE_IP"

    return {
        "order_id": f"O{datetime.now().strftime('%Y%m%d%H%M%S')}{sequence:06d}",
        "user_id": user_id,
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "city": random.choice(CITIES),
        "amount": amount,
        "quantity": quantity,
        "payment_status": payment_status,
        "device_id": device_id,
        "ip_address": ip_address,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate realtime ecommerce order stream.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between orders.")
    parser.add_argument("--total", type=int, default=0, help="0 means infinite stream.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatable demos.")
    parser.add_argument("--quiet", action="store_true", help="Do not print every generated order.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    serve(args.host, args.port, args.interval, args.total, args.quiet)
