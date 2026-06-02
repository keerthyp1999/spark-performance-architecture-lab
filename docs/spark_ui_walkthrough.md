# Spark UI Walkthrough

## 1. Jobs Tab

Start here.

The Jobs tab answers:

```text
Which action was expensive?
```

Look at:

- job description
- duration
- number of stages
- succeeded/failed jobs

## 2. Stages Tab

Stages answer:

```text
Why was the job expensive?
```

Look at:

- duration
- tasks
- input
- shuffle read
- shuffle write
- spill

## 3. Tasks Table

Tasks answer:

```text
Which partition caused the issue?
```

Look for:

- one task much slower than others
- one task reading much more data
- high GC time
- failed attempts

## 4. SQL Tab

SQL tab shows physical execution plan.

Look for:

- Exchange
- BroadcastHashJoin
- SortMergeJoin
- HashAggregate
- Sort

## 5. Executors Tab

Executors tab shows worker health.

Look for:

- GC time
- storage memory
- shuffle read/write
- failed tasks
- executor imbalance

## 6. Storage Tab

Use this after cache/persist.

It shows how much memory/disk cached data used.

## 7. Environment Tab

Use this to confirm Spark configuration.

Important configs:

```text
spark.sql.shuffle.partitions
spark.sql.adaptive.enabled
spark.sql.autoBroadcastJoinThreshold
spark.executor.memory
spark.driver.memory
```
