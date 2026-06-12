import unittest

from flink_jobs.order_stream_job import (
    AMOUNT,
    DEVICE_ID,
    EVENT_TIME,
    IP_ADDRESS,
    PAYMENT_STATUS,
    build_order_alert,
    build_shared_identity_alerts,
    build_user_behavior_alerts,
    has_order_alert,
    parse_event_time_millis,
)


def make_order(
    order_id="O1",
    user_id="U1",
    amount=9999.0,
    payment_status="SUCCESS",
    device_id="D1001",
    ip_address="192.168.1.10",
    city="上海",
    event_time="2026-06-12 10:00:00",
):
    return (
        order_id,
        user_id,
        "P1001",
        "无线耳机",
        "数码",
        city,
        amount,
        1,
        payment_status,
        device_id,
        ip_address,
        event_time,
    )


class OrderStreamRuleTests(unittest.TestCase):
    def test_high_amount_alone_does_not_trigger_alert(self):
        order = make_order(amount=12000.0)

        self.assertFalse(has_order_alert(order))

    def test_failed_payment_and_risky_identity_trigger_order_alert(self):
        order = make_order(payment_status="FAILED", device_id="D-RISK-001", ip_address="10.10.8.8")

        self.assertTrue(has_order_alert(order))
        alert = build_order_alert(order)

        self.assertIn("PAYMENT_FAILED", alert[3])
        self.assertIn("RISKY_DEVICE", alert[3])
        self.assertIn("RISKY_IP", alert[3])
        self.assertEqual(alert[4], "HIGH")

    def test_user_behavior_window_builds_compound_alerts(self):
        orders = [
            make_order(f"O{idx}", "U-1", payment_status="FAILED", device_id=f"D-HOP-{idx}", city=city)
            for idx, city in enumerate(["北京", "上海", "广州", "深圳"], start=1)
        ]

        alerts = build_user_behavior_alerts(
            "U-1",
            orders,
            "2026-06-12 10:00:00",
            "2026-06-12 10:00:10",
            1781239210000,
            frequency_threshold=4,
            failed_payment_threshold=3,
            device_hop_threshold=3,
            city_hop_threshold=3,
        )
        alert_types = {alert[3] for alert in alerts}

        self.assertEqual(
            alert_types,
            {"FREQUENT_USER_ORDERS", "REPEATED_PAYMENT_FAILURES", "DEVICE_HOPPING", "GEO_VELOCITY"},
        )

    def test_shared_ip_window_alert(self):
        orders = [
            make_order(f"O{idx}", f"U{idx}", ip_address="172.20.88.8")
            for idx in range(1, 5)
        ]

        alerts = build_shared_identity_alerts(
            "172.20.88.8",
            orders,
            "2026-06-12 10:00:00",
            "2026-06-12 10:00:10",
            1781239210000,
            threshold=4,
            alert_prefix="SHAREDIP",
            alert_type="SHARED_IP_USERS",
            identity_label="IP",
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0][3], "SHARED_IP_USERS")

    def test_event_time_millis(self):
        millis = parse_event_time_millis("2026-06-12 10:00:00")

        self.assertIsInstance(millis, int)
        self.assertGreater(millis, 0)


if __name__ == "__main__":
    unittest.main()
