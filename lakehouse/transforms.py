"""Bronze -> silver -> gold transformations (medallion architecture) in PySpark.

Every function takes and returns DataFrames, so each step is unit-testable without touching storage.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("warehouse", StringType()),
])


# ---------- bronze: read raw files with explicit schemas ----------

def read_bronze(spark: SparkSession, bronze_dir: str) -> dict[str, DataFrame]:
    return {
        "orders": spark.read.schema(ORDERS_SCHEMA).json(f"{bronze_dir}/orders.json"),
        "products": spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze_dir}/products.csv"),
        "shipments": spark.read.option("header", True).csv(f"{bronze_dir}/shipments.csv"),
    }


# ---------- silver: typed, deduplicated, validated ----------

def clean_orders(orders: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid_orders, quarantined_orders). Bad rows are kept with a reason, never silently dropped."""
    deduped = orders.dropDuplicates()
    known = products.select("product_id").withColumn("_known", F.lit(True))
    checked = (
        deduped.withColumn("order_date", F.to_date("order_date"))
        .join(known, "product_id", "left")
        .withColumn(
            "reject_reason",
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
            .when(F.col("_known").isNull(), "unknown_product")
            .when(F.col("order_date").isNull(), "invalid_date"),
        )
        .drop("_known")
    )
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    quarantined = checked.filter(F.col("reject_reason").isNotNull())
    return valid, quarantined


def clean_shipments(shipments: DataFrame) -> DataFrame:
    date_cols = ["shipped_date", "promised_date", "delivered_date"]
    out = shipments.dropDuplicates(["order_id"])
    for c in date_cols:
        out = out.withColumn(c, F.to_date(c).cast(DateType()))
    return out


# ---------- gold: dimensional model + KPI mart ----------

def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select("product_id", "product_name", "category", F.col("unit_cost").cast("double"))


def build_fct_shipments(orders: DataFrame, shipments: DataFrame, dim_product: DataFrame) -> DataFrame:
    """One row per order shipment, with delivery performance flags."""
    return (
        orders.join(shipments, "order_id", "left")
        .join(dim_product.select("product_id", "unit_cost"), "product_id", "left")
        .withColumn("order_value", F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("is_delivered", F.col("delivered_date").isNotNull())
        .withColumn(
            "is_on_time",
            F.when(F.col("delivered_date").isNull(), None).otherwise(F.col("delivered_date") <= F.col("promised_date")),
        )
        .withColumn("days_late", F.greatest(F.datediff("delivered_date", "promised_date"), F.lit(0)))
        .select("order_id", "order_date", "product_id", "warehouse", "carrier", "quantity", "order_value",
                "shipped_date", "promised_date", "delivered_date", "is_delivered", "is_on_time", "days_late")
    )


def build_carrier_kpis(fct: DataFrame) -> DataFrame:
    """Monthly on-time-in-full style KPIs by warehouse and carrier."""
    delivered = fct.filter("is_delivered")
    return (
        delivered.withColumn("month", F.date_trunc("month", "order_date").cast(DateType()))
        .groupBy("month", "warehouse", "carrier")
        .agg(
            F.count("*").alias("deliveries"),
            F.round(F.avg(F.col("is_on_time").cast("int")) * 100, 1).alias("on_time_pct"),
            F.round(F.avg("days_late"), 2).alias("avg_days_late"),
            F.round(F.sum("order_value"), 2).alias("delivered_value"),
        )
        .orderBy("month", "warehouse", "carrier")
    )


# ---------- data quality checks ----------

def quality_checks(fct: DataFrame, quarantined: DataFrame, raw_orders: DataFrame) -> dict[str, bool]:
    """Simple assertions a pipeline should fail on. Returns check name -> passed."""
    total = fct.count()
    return {
        "fct_has_rows": total > 0,
        "order_id_unique": fct.select("order_id").distinct().count() == total,
        "no_null_order_value": fct.filter(F.col("order_value").isNull()).count() == 0,
        "quarantine_below_5pct": quarantined.count() <= 0.05 * raw_orders.count(),
    }
