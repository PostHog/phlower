"""Send fake Celery tasks to Redis to exercise phlower end-to-end.

Usage: uv run python scripts/fake_tasks.py
"""

from __future__ import annotations

import random
import time

from celery import Celery

app = Celery(broker="redis://localhost:6379/0")

TASK_NAMES = [
    "myapp.tasks.send_email",
    "myapp.tasks.generate_invoice",
    "myapp.tasks.process_order",
    "myapp.tasks.sync_inventory",
    "myapp.tasks.send_notification",
]


@app.task(name="myapp.tasks.send_email")
def send_email(to: str, subject: str):
    time.sleep(random.uniform(0.01, 0.3))
    if random.random() < 0.05:
        raise ValueError(f"Invalid email: {to}")
    return "sent"


@app.task(name="myapp.tasks.generate_invoice")
def generate_invoice(order_id: int):
    time.sleep(random.uniform(0.1, 2.0))
    if random.random() < 0.1:
        raise RuntimeError(f"PDF generation failed for order {order_id}")
    return {"invoice_id": random.randint(1000, 9999)}


@app.task(name="myapp.tasks.process_order")
def process_order(order_id: int, items: list):
    time.sleep(random.uniform(0.05, 0.5))
    if random.random() < 0.03:
        raise ConnectionError("Payment gateway timeout")
    return "processed"


@app.task(name="myapp.tasks.sync_inventory")
def sync_inventory(sku: str):
    time.sleep(random.uniform(0.02, 0.1))
    return "synced"


@app.task(name="myapp.tasks.send_notification")
def send_notification(user_id: int, message: str):
    time.sleep(random.uniform(0.01, 0.05))
    if random.random() < 0.15:
        raise TimeoutError("Push service unavailable")
    return "delivered"


def main():
    print("Sending tasks to Redis broker... (Ctrl+C to stop)")
    print("Make sure a celery worker is running:")
    print("  uv run celery -A scripts.fake_tasks worker -E -l info\n")

    while True:
        name = random.choice(TASK_NAMES)
        if name == "myapp.tasks.send_email":
            send_email.delay(
                to=f"user{random.randint(1, 100)}@example.com",
                subject=f"Order #{random.randint(1000, 9999)} confirmation",
            )
        elif name == "myapp.tasks.generate_invoice":
            generate_invoice.delay(order_id=random.randint(1000, 9999))
        elif name == "myapp.tasks.process_order":
            process_order.delay(
                order_id=random.randint(1000, 9999),
                items=[f"item-{i}" for i in range(random.randint(1, 5))],
            )
        elif name == "myapp.tasks.sync_inventory":
            sync_inventory.delay(sku=f"SKU-{random.randint(100, 999)}")
        elif name == "myapp.tasks.send_notification":
            send_notification.delay(
                user_id=random.randint(1, 500),
                message="Your order has shipped!",
            )

        time.sleep(random.uniform(0.1, 0.5))


if __name__ == "__main__":
    main()
