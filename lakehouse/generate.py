"""Generate a small, deliberately messy supply-chain dataset as the bronze (raw) layer.

Pure Python so it runs anywhere; the Spark jobs read the files it writes.
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

WAREHOUSES = ["LIS", "MAD", "BER", "WAW"]
CARRIERS = ["DHL", "UPS", "DPD", "GLS"]


def generate(out_dir: str | Path, n_orders: int = 2000, seed: int = 7) -> Path:
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    products = [
        {"product_id": f"P{i:03d}", "product_name": f"Product {i}", "category": rng.choice(["toys", "home", "sports"]),
         "unit_cost": round(rng.uniform(2, 60), 2)}
        for i in range(1, 51)
    ]
    with (out / "products.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(products[0]))
        writer.writeheader()
        writer.writerows(products)

    start = date(2026, 1, 1)
    orders, shipments = [], []
    for i in range(1, n_orders + 1):
        order_date = start + timedelta(days=rng.randint(0, 270))
        order = {
            "order_id": f"O{i:05d}",
            "order_date": order_date.isoformat(),
            "product_id": rng.choice(products)["product_id"],
            "quantity": rng.randint(1, 20),
            "warehouse": rng.choice(WAREHOUSES),
        }
        # Messy data, as real sources are: blank quantities, unknown products, duplicate events.
        if rng.random() < 0.02:
            order["quantity"] = None
        if rng.random() < 0.01:
            order["product_id"] = "P999"
        orders.append(order)
        if rng.random() < 0.03:
            orders.append(dict(order))  # exact duplicate from a replayed feed

        promised = order_date + timedelta(days=3)
        delay = rng.choices([0, 1, 2, 5], weights=[70, 15, 10, 5])[0]
        shipped = order_date + timedelta(days=rng.randint(0, 2))
        shipments.append({
            "order_id": order["order_id"],
            "carrier": rng.choice(CARRIERS),
            "shipped_date": shipped.isoformat(),
            "promised_date": promised.isoformat(),
            "delivered_date": (promised + timedelta(days=delay - 1)).isoformat() if rng.random() > 0.04 else None,
        })

    (out / "orders.json").write_text("\n".join(json.dumps(o) for o in orders), encoding="utf-8")
    with (out / "shipments.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(shipments[0]))
        writer.writeheader()
        writer.writerows(shipments)
    return out


if __name__ == "__main__":
    print(generate("data/bronze"))
