"""Run the full pipeline: python -m lakehouse.pipeline [--base data]

Writes silver and gold layers as Parquet (swap .parquet for .format("delta") on Databricks).
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from .generate import generate
from .transforms import (build_carrier_kpis, build_dim_product, build_fct_shipments, clean_orders,
                         clean_shipments, quality_checks, read_bronze)


def run(spark: SparkSession, base: str) -> dict[str, bool]:
    bronze = read_bronze(spark, f"{base}/bronze")
    orders, quarantined = clean_orders(bronze["orders"], bronze["products"])
    shipments = clean_shipments(bronze["shipments"])
    dim_product = build_dim_product(bronze["products"])
    fct = build_fct_shipments(orders, shipments, dim_product)
    kpis = build_carrier_kpis(fct)

    orders.write.mode("overwrite").parquet(f"{base}/silver/orders")
    quarantined.write.mode("overwrite").parquet(f"{base}/silver/orders_quarantine")
    shipments.write.mode("overwrite").parquet(f"{base}/silver/shipments")
    dim_product.write.mode("overwrite").parquet(f"{base}/gold/dim_product")
    fct.write.mode("overwrite").partitionBy("warehouse").parquet(f"{base}/gold/fct_shipments")
    kpis.write.mode("overwrite").parquet(f"{base}/gold/carrier_kpis_monthly")

    checks = quality_checks(fct, quarantined, bronze["orders"])
    kpis.show(10, truncate=False)
    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="data")
    args = parser.parse_args()
    generate(f"{args.base}/bronze")
    spark = SparkSession.builder.appName("lakehouse").master("local[*]").getOrCreate()
    try:
        checks = run(spark, args.base)
    finally:
        spark.stop()
    for name, passed in checks.items():
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
