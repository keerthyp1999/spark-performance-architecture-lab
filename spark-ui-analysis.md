# Spark UI Analysis Guide

Use this document after each Glue run. Add screenshots from your own run in the `screenshots/` folder.

## 1. Jobs tab

Question to ask:

```text
Which action created this job and how long did it take?
```

Common descriptions:

| Description | Meaning |
|---|---|
| `showString` | `df.show()` |
| `count` | `df.count()` |
| `parquet` | Read/write parquet |
| `javaToPython` | Data crossing JVM/Python boundary |

## 2. Stages tab

Question to ask:

```text
Where did shuffle happen?
```

Look at:

- Duration
- Tasks
- Shuffle Read
- Shuffle Write
- Spill
- GC Time

## 3. Tasks inside a stage

Question to ask:

```text
Are all tasks similar or is one task much slower?
```

Healthy:

```text
Task durations are similar
Shuffle read sizes are similar
```

Unhealthy:

```text
One task reads much more data than others
One task runs much longer
```

This usually indicates skew.

## 4. SQL tab

Question to ask:

```text
What physical strategy did Spark choose?
```

Look for:

| Plan Term | Meaning |
|---|---|
| BroadcastHashJoin | Small table copied to executors |
| SortMergeJoin | Big table join with shuffle and sort |
| Exchange | Shuffle boundary |
| HashAggregate | Grouping/aggregation |
| Sort | Sorting step |

## 5. Executors tab

Question to ask:

```text
Are executors healthy?
```

Look for:

- GC Time
- Failed Tasks
- Shuffle Read/Write
- Storage Memory
- Input data

## 6. Storage tab

Only useful when `cache()` or `persist()` is used.

Question to ask:

```text
Did the cached data fit in memory?
```

## 7. Environment tab

Confirm actual configs:

```text
spark.sql.shuffle.partitions
spark.sql.adaptive.enabled
spark.executor.memory
spark.driver.memory
spark.sql.autoBroadcastJoinThreshold
```

## Improvement checklist

| Spark UI Symptom | Likely Issue | Improvement |
|---|---|---|
| High shuffle read/write | Expensive redistribution | Broadcast small table, reduce wide operations |
| One slow task | Skew | Salt key, repartition, AQE skew handling |
| High spill | Memory pressure | Increase memory, tune partitions |
| High GC | JVM memory pressure | Reduce cache, reduce object creation, adjust memory |
| Many tiny tasks | Over-partitioned | Reduce shuffle partitions |
| Few huge tasks | Under-partitioned | Increase shuffle partitions |
