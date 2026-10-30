import datetime as dt

import pytest
from pyspark.sql import SparkSession

from lakehouse.generate import generate
from lakehouse.pipeline import run
from lakehouse.transforms import (ORDERS_SCHEMA, build_carrier_kpis, build_dim_product, build_fct_shipments,
                                  clean_orders, clean_shipments)


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def products(spark):
    return spark.createDataFrame([("P1", "Ball", "toys", 2.5), ("P2", "Lamp", "home", 10.0)],
                                 ["product_id", "product_name", "category", "unit_cost"])


def orders_df(spark, rows):
    return spark.createDataFrame(rows, ORDERS_SCHEMA)


def test_clean_orders_dedupes_and_quarantines(spark, products):
    raw = orders_df(spark, [
        ("O1", "2026-03-01", "P1", 4, "LIS"),
        ("O1", "2026-03-01", "P1", 4, "LIS"),      # duplicate
        ("O2", "2026-03-02", "P9", 1, "LIS"),      # unknown product
        ("O3", "2026-03-02", "P2", None, "MAD"),   # missing quantity
    ])
    valid, quarantined = clean_orders(raw, products)
    assert [r.order_id for r in valid.collect()] == ["O1"]
    reasons = {r.order_id: r.reject_reason for r in quarantined.collect()}
    assert reasons == {"O2": "unknown_product", "O3": "invalid_quantity"}


def test_fact_flags_on_time_and_late(spark, products):
    orders = orders_df(spark, [("O1", "2026-03-01", "P1", 4, "LIS"), ("O2", "2026-03-01", "P2", 1, "LIS"),
                               ("O3", "2026-03-01", "P2", 1, "LIS")])
    orders, _ = clean_orders(orders, products)
    shipments = clean_shipments(spark.createDataFrame([
        ("O1", "DHL", "2026-03-01", "2026-03-04", "2026-03-03"),
        ("O2", "UPS", "2026-03-02", "2026-03-04", "2026-03-06"),
        ("O3", "UPS", "2026-03-02", "2026-03-04", None),
    ], ["order_id", "carrier", "shipped_date", "promised_date", "delivered_date"]))
    fct = {r.order_id: r for r in build_fct_shipments(orders, shipments, build_dim_product(products)).collect()}

    assert fct["O1"].is_on_time is True and fct["O1"].days_late == 0
    assert fct["O1"].order_value == 10.0
    assert fct["O2"].is_on_time is False and fct["O2"].days_late == 2
    assert fct["O3"].is_delivered is False and fct["O3"].is_on_time is None


def test_kpis_aggregate_delivered_only(spark, products):
    orders, _ = clean_orders(orders_df(spark, [("O1", "2026-03-01", "P1", 4, "LIS"),
                                               ("O2", "2026-03-09", "P1", 2, "LIS")]), products)
    shipments = clean_shipments(spark.createDataFrame([
        ("O1", "DHL", "2026-03-01", "2026-03-04", "2026-03-03"),
        ("O2", "DHL", "2026-03-09", "2026-03-12", "2026-03-14"),
    ], ["order_id", "carrier", "shipped_date", "promised_date", "delivered_date"]))
    kpi = build_carrier_kpis(build_fct_shipments(orders, shipments, build_dim_product(products))).collect()[0]
    assert kpi.month == dt.date(2026, 3, 1)
    assert kpi.deliveries == 2 and kpi.on_time_pct == 50.0 and kpi.avg_days_late == 1.0


def test_end_to_end_pipeline_passes_quality_checks(spark, tmp_path):
    generate(tmp_path / "bronze", n_orders=300)
    checks = run(spark, str(tmp_path))
    assert all(checks.values()), checks
    assert (tmp_path / "gold" / "carrier_kpis_monthly").exists()
