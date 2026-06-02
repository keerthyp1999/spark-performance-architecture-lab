import argparse
from pyspark.sql import SparkSession, DataFrame
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


def parse_args():
    parser = argparse.ArgumentParser(description="Local Spark Performance Architecture Lab")
    parser.add_argument("--row-count", type=int, default=100_000)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--output-path", default="output/sample_results")
    parser.add_argument("--skew-mode", default="true")
    parser.add_argument("--schema-drift-mode", default="true")
    parser.add_argument("--quality-fail-fast", default="false")
    return parser.parse_args()


def as_bool(value: str) -> bool:
    return str(value).lower() in {"true", "1", "yes", "y"}


def expected_orders_schema() -> StructType:
    return StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("country", StringType(), True),
    ])


def align_to_expected_schema(df: DataFrame, schema: StructType) -> DataFrame:
    current_columns = set(df.columns)
    expected_columns = {field.name for field in schema.fields}

    for field in schema.fields:
        if field.name not in current_columns:
            print(f"SCHEMA_DRIFT: missing expected column {field.name}; adding null")
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    extra_columns = [c for c in df.columns if c not in expected_columns]
    if extra_columns:
        print(f"SCHEMA_DRIFT: dropping unexpected columns from curated output: {extra_columns}")

    return df.select([field.name for field in schema.fields])


def valid_orders(df: DataFrame) -> DataFrame:
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


def log_section(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


args = parse_args()
skew_mode = as_bool(args.skew_mode)
schema_drift_mode = as_bool(args.schema_drift_mode)
quality_fail_fast = as_bool(args.quality_fail_fast)

spark = (
    SparkSession.builder
    .appName("SparkPerformanceArchitectureLabLocal")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    .getOrCreate()
)

log_section("LOCAL RUN PARAMETERS")
print("ROW_COUNT:", args.row_count)
print("SHUFFLE_PARTITIONS:", args.shuffle_partitions)
print("OUTPUT_PATH:", args.output_path)
print("SKEW_MODE:", skew_mode)
print("SCHEMA_DRIFT_MODE:", schema_drift_mode)
print("QUALITY_FAIL_FAST:", quality_fail_fast)

country_expr = (
    when(rand() < 0.70, lit("US"))
    .when(rand() < 0.85, lit("IN"))
    .when(rand() < 0.95, lit("UK"))
    .otherwise(lit("CA"))
)

if skew_mode:
    product_expr = when(rand() < 0.55, lit(1)).otherwise((floor(rand() * 1000)).cast("int"))
else:
    product_expr = (floor(rand() * 1000)).cast("int")

log_section("1. GENERATE SYNTHETIC RAW DATA")
orders_raw = (
    spark.range(0, args.row_count)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (floor(rand() * 100000)).cast("int"))
    .withColumn("product_id", product_expr)
    .withColumn("amount", (rand() * 500).cast("double"))
    .withColumn("country", country_expr)
    .withColumn("amount", when(col("order_id") % 997 == 0, -spark_abs(col("amount"))).otherwise(col("amount")))
    .withColumn("customer_id", when(col("order_id") % 1237 == 0, lit(None).cast("int")).otherwise(col("customer_id")))
    .withColumn("country", when(col("order_id") % 2089 == 0, lit("UNKNOWN")).otherwise(col("country")))
)

if schema_drift_mode:
    orders_raw = orders_raw.withColumn("coupon_code", concat(lit("CPN_"), (col("order_id") % 5)))

products = (
    spark.range(0, 1000)
    .withColumnRenamed("id", "product_id")
    .withColumn("category", concat(lit("category_"), (col("product_id") % 10)))
)

orders_raw.printSchema()

log_section("2. SCHEMA DRIFT HANDLING")
orders = align_to_expected_schema(orders_raw, expected_orders_schema())
orders.printSchema()

log_section("3. DATA QUALITY CHECKS")
total = orders.count()
dq_rows = [
    ("orders", "null_order_id", orders.filter(col("order_id").isNull()).count(), total),
    ("orders", "null_customer_id", orders.filter(col("customer_id").isNull()).count(), total),
    ("orders", "null_product_id", orders.filter(col("product_id").isNull()).count(), total),
    ("orders", "null_amount", orders.filter(col("amount").isNull()).count(), total),
    ("orders", "negative_amount", orders.filter(col("amount") < 0).count(), total),
    ("orders", "invalid_country", orders.filter(~col("country").isin("US", "IN", "UK", "CA")).count(), total),
    ("orders", "duplicate_order_id", total - orders.select("order_id").dropDuplicates().count(), total),
]
dq_report = spark.createDataFrame(dq_rows, ["dataset", "check_name", "failed_count", "total_count"]).withColumn("checked_at", current_timestamp())
dq_report.show(50, False)

failed_total = dq_report.agg(spark_sum("failed_count").alias("failed_total")).collect()[0]["failed_total"]
if quality_fail_fast and failed_total > 0:
    raise RuntimeError(f"Data quality failed with {failed_total} failed checks")

orders_clean = valid_orders(orders)
orders_invalid = invalid_orders(orders)

print("Raw count:", total)
print("Clean count:", orders_clean.count())
print("Invalid/quarantine count:", orders_invalid.count())

log_section("4. PARTITION DISTRIBUTION BEFORE REPARTITION")
print("Partitions before repartition:", orders_clean.rdd.getNumPartitions())
orders_clean.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().orderBy("partition_id").show(100, False)

log_section("5. REPARTITION BY product_id")
orders_repartitioned = orders_clean.repartition(args.shuffle_partitions, "product_id")
print("Partitions after repartition:", orders_repartitioned.rdd.getNumPartitions())
orders_repartitioned.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().orderBy("partition_id").show(100, False)

orders_repartitioned.cache()
print("Cached count:", orders_repartitioned.count())

log_section("6. BROADCAST JOIN")
joined = orders_repartitioned.join(broadcast(products), "product_id", "left")
print("Joined count:", joined.count())

log_section("7. AGGREGATION")
agg = joined.groupBy("country", "category").agg(count("*").alias("order_count"), spark_sum("amount").alias("total_amount"))
agg.orderBy(col("total_amount").desc()).show(100, False)

log_section("8. WRITE OUTPUT")
agg.orderBy(col("total_amount").desc()).write.mode("overwrite").parquet(f"{args.output_path}/aggregates/revenue_by_country_category")
orders_clean.write.mode("overwrite").parquet(f"{args.output_path}/curated/orders_clean")
orders_invalid.write.mode("overwrite").parquet(f"{args.output_path}/quarantine/orders_invalid")
dq_report.write.mode("overwrite").parquet(f"{args.output_path}/quality/dq_report")
print("Local job completed. Output root:", args.output_path)

spark.stop()
