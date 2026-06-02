# Partitions and Repartition

## Partition

A partition is a chunk of distributed data.

Each task usually processes one partition.

## Check Number of Partitions

```python
df.rdd.getNumPartitions()
```

## Check Rows Per Partition

```python
from pyspark.sql.functions import spark_partition_id

 df.withColumn("partition_id", spark_partition_id()) \
   .groupBy("partition_id") \
   .count() \
   .show()
```

## Repartition

`repartition()` creates a full shuffle.

```python
df.repartition(20, "product_id")
```

Spark:

1. reads current partitions
2. hashes the repartition key
3. moves rows across executors
4. creates new balanced partitions

## Why Repartition Helps

Repartition does not reduce scanning. It reorganizes data for better future processing.

It helps with:

- better parallelism
- reducing skew
- aligning data by join/group key
- controlling output file count

## Coalesce

`coalesce()` is usually used to reduce partitions with less shuffle.
