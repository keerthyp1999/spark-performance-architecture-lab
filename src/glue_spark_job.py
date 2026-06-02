import sys
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    broadcast,
    col,
    concat,
    count,
    current_timestamp,
    floor,
    lit,
    rand,
    spark_partition_id,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

"""
AWS Glue PySpark job for learning Spark internals and production-style ETL patterns.

No source database, crawler, or table is required. The job generates synthetic data inside Spark.

Required Glue parameter:
--OUTPUT_PATH s3://your-bucket/spark-performance-lab/output/

Optional Glue parameters:
--DATA_SIZE small|medium|large
--SHUFFLE_PARTITIONS 20
--SKEW_MODE true|false
--SCHEMA_DRIFT_MODE true|false
--QUALITY_FAIL_FAST false

What this job demonstrates:
- worker/memory scaling experiments
- partitions, repartition, tasks, and shuffle
- broadcast joins
- aggregation and sorting
- cache/storage memory
- schema drift handling
- data quality checks and quarantine output
- Spark UI debugging through Jobs, Stages, SQL, Executors, and Storage tabs
"""

REQUIRED_ARGS = ["JOB_NAME", "OUTPUT_PATH"]
OPTIONAL_ARGS_WITH_DEFAULTS = {
    "DATA_SIZE": "medium",
    "SHUFFLE_PARTITIONS": "20",
    "SKEW_MODE": "true",
    "SCHEMA_DRIFT_MODE": "true",
    "QUALITY_FAIL_FAST": "false",
}


def get_glue_args():
    """Read required Glue args and optional args if they were provided."""
    provided_keys = {arg.replace("--", "") for arg in sys.argv if arg.startswith("--")}
    optional_present = [key for key in OPTIONAL_ARGS_WITH_DEFAULTS if key in provided_keys]
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS + optional_present)
    for key, default_value in OPTIONAL_ARGS_WITH_DEFAULTS.items():
        args.setdefault(key, default_value)
    return args


def bool_arg(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def log_section(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def rows_for_size(data_size: str) -> int:
    sizes = {
        "small": 100_000,
        "medium": 1_000_000,
        "large": 5_000_000,
    }
    return sizes.get(data_size.lower(), 1_000_000)


def expected_orders_schema() -> StructType:
    return StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("country", StringType(), True),
        ]
    )


def align_to_expected_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Schema drift handling strategy.
    - Missing expected columns are added as nulls.
    - Expected columns are cast to expected types.
    - Extra unexpected columns are dropped from the curated dataset.
    """
    current_columns = set(df.columns)
    for field in schema.fields:
        if field.name not in current_columns:
            print(f"SCHEMA_DRIFT: missing expected column {field.name}; adding null column")
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    extra_columns = [c for c in df.columns if c not in {field.name for field in schema.fields}]
    if extra_columns:
        print(f"SCHEMA_DRIFT: dropping unexpected columns from curated data: {extra_columns}")

    return df.select([field.name for field in schema.fields])


def quality_report(df: DataFrame, name: str) -> DataFrame:
    """Create a compact data quality report."""
    total = df.count()
    checks = [
        ("null_order_id", df.filter(col("order_id").isNull()).count()),
        ("null_customer_id", df.filter(col("customer_id").isNull()).count()),
        ("null_product_id", df.filter(col("product_id").isNull()).count()),
        ("null_amount", df.filter(col("amount").isNull()).count()),
        ("negative_amount", df.filter(col("amount") < 0).count()),
        ("invalid_country", df.filter(~col("country").isin("US", "IN", "UK", "CA")).count()),
        ("duplicate_order_id", total - df.select("order_id").dropDuplicates().count()),
    ]

    rows = [(name, check_name, int(failed_count), int(total), current_timestamp()) for check_name, failed_count in checks]
    return spark.createDataFrame(rows, ["dataset", "check_name", "failed_count", "total_count", "checked_at"])


def valid_orders(df: DataFrame) -> DataFrame:
    """Apply core quality rules before business aggregation."""
    return (
        df.dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("amount") >= 0)
        .filter(col("country").isin("US", "IN", "UK", "CA"))
    )


def invalid_orders(df: DataFrame) -> DataFrame:
    return df.filter(
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
        | col("amount").isNull()
        | (col("amount") < 0)
        | (~col("country").isin("US", "IN", "UK", "CA"))
    )


args = get_glue_args()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

output_path = args["OUTPUT_PATH"].rstrip("/")
data_size = args["DATA_SIZE"].lower()
row_count = rows_for_size(data_size)
shuffle_partitions = int(args["SHUFFLE_PARTITIONS"])
skew_mode = bool_arg(args["SKEW_MODE"])
schema_drift_mode = bool_arg(args["SCHEMA_DRIFT_MODE"])
quality_fail_fast = bool_arg(args["QUALITY_FAIL_FAST"])

spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

log_section("SPARK CONFIGS AND EXPERIMENT PARAMETERS")
print("DATA_SIZE:", data_size)
print("ROW_COUNT:", row_count)
print("SHUFFLE_PARTITIONS:", shuffle_partitions)
print("SKEW_MODE:", skew_mode)
print("SCHEMA_DRIFT_MODE:", schema_drift_mode)
print("QUALITY_FAIL_FAST:", quality_fail_fast)
print("spark.sql.shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("spark.sql.adaptive.enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("spark.sql.adaptive.skewJoin.enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("spark.sql.autoBroadcastJoinThreshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# -------------------------------------------------------------------
# 1. Generate synthetic raw data with intentional data quality issues
# -------------------------------------------------------------------
log_section("1. GENERATE SYNTHETIC RAW DATA")

country_expr = (
    when(rand() < 0.70, lit("US"))
    .when(rand() < 0.85, lit("IN"))
    .when(rand() < 0.95, lit("UK"))
    .otherwise(lit("CA"))
)

if skew_mode:
    # Create intentional product skew. Many rows go to product_id 1.
    product_expr = when(rand() < 0.55, lit(1)).otherwise((floor(rand() * 1000)).cast("int"))
else:
    product_expr = (floor(rand() * 1000)).cast("int")

orders_raw = (
    spark.range(0, row_count)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (floor(rand() * 100000)).cast("int"))
    .withColumn("product_id", product_expr)
    .withColumn("amount", (rand() * 500).cast("double"))
    .withColumn("country", country_expr)
)

# Inject quality issues.
orders_raw = (
    orders_raw
    .withColumn("amount", when(col("order_id") % 997 == 0, -spark_abs(col("amount"))).otherwise(col("amount")))
    .withColumn("customer_id", when(col("order_id") % 1237 == 0, lit(None).cast("int")).otherwise(col("customer_id")))
    .withColumn("country", when(col("order_id") % 2089 == 0, lit("UNKNOWN")).otherwise(col("country")))
)

if schema_drift_mode:
    orders_raw = orders_raw.withColumn("coupon_code", concat(lit("CPN_"), (col("order_id") % 5)))
    print("SCHEMA_DRIFT: added unexpected column coupon_code to raw data")

products = (
    spark.range(0, 1000)
    .withColumnRenamed("id", "product_id")
    .withColumn("category", concat(lit("category_"), (col("product_id") % 10)))
)

print("Orders raw partitions:", orders_raw.rdd.getNumPartitions())
print("Products partitions:", products.rdd.getNumPartitions())

# -------------------------------------------------------------------
# 2. Handle schema drift
# -------------------------------------------------------------------
log_section("2. SCHEMA DRIFT HANDLING")
orders_aligned = align_to_expected_schema(orders_raw, expected_orders_schema())
orders_aligned.printSchema()

# -------------------------------------------------------------------
# 3. Data quality checks and quarantine
# -------------------------------------------------------------------
log_section("3. DATA QUALITY CHECKS")
dq_report = quality_report(orders_aligned, "orders_aligned")
dq_report.show(50, False)

failed_total = dq_report.agg(spark_sum("failed_count").alias("failed_total")).collect()[0]["failed_total"]
print("Total failed quality checks:", failed_total)
if quality_fail_fast and failed_total > 0:
    raise Exception(f"Data quality failed with {failed_total} failed records/checks")

orders_invalid = invalid_orders(orders_aligned)
orders_clean = valid_orders(orders_aligned)

print("Raw orders count:", orders_aligned.count())
print("Clean orders count:", orders_clean.count())
print("Invalid/quarantined orders count:", orders_invalid.count())

# -------------------------------------------------------------------
# 4. Inspect partitions before repartition
# -------------------------------------------------------------------
log_section("4. PARTITION DISTRIBUTION BEFORE REPARTITION")
orders_clean.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy("partition_id") \
    .show(100, False)

# -------------------------------------------------------------------
# 5. Repartition by product_id to demonstrate shuffle and skew handling
# -------------------------------------------------------------------
log_section("5. REPARTITION BY product_id")
orders_repartitioned = orders_clean.repartition(shuffle_partitions, "product_id")
print("Orders partitions after repartition:", orders_repartitioned.rdd.getNumPartitions())

orders_repartitioned.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy("partition_id") \
    .show(100, False)

# Cache intentionally so Storage tab can be inspected.
orders_repartitioned.cache()
print("Cached repartitioned orders count:", orders_repartitioned.count())

# -------------------------------------------------------------------
# 6. Broadcast join with product lookup
# -------------------------------------------------------------------
log_section("6. BROADCAST JOIN")
broadcast_joined = orders_repartitioned.join(
    broadcast(products),
    on="product_id",
    how="left"
)
print("Broadcast joined count:", broadcast_joined.count())

# -------------------------------------------------------------------
# 7. Aggregation to trigger shuffle/hash aggregate
# -------------------------------------------------------------------
log_section("7. AGGREGATION")
agg_df = (
    broadcast_joined
    .groupBy("country", "category")
    .agg(
        count("*").alias("order_count"),
        spark_sum("amount").alias("total_amount")
    )
)
agg_df.show(100, False)

# -------------------------------------------------------------------
# 8. Write curated, quarantine, quality report, and aggregate output
# -------------------------------------------------------------------
log_section("8. WRITE OUTPUTS")
agg_df.orderBy(col("total_amount").desc()).write.mode("overwrite").parquet(f"{output_path}/aggregates/revenue_by_country_category")
orders_clean.write.mode("overwrite").parquet(f"{output_path}/curated/orders_clean")
orders_invalid.write.mode("overwrite").parquet(f"{output_path}/quarantine/orders_invalid")
dq_report.write.mode("overwrite").parquet(f"{output_path}/quality/dq_report")

print("Job completed successfully. Output root:", output_path)
time.sleep(30)
