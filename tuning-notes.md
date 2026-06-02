# Tuning Notes

## What to tune first

Do not tune randomly. Use Spark UI evidence.

```text
Jobs tab      → Which action is slow?
Stages tab    → Which stage is expensive?
Tasks tab     → Are partitions balanced?
Executors tab → Is memory or GC a problem?
SQL tab       → What physical plan was chosen?
```

## Worker scaling

Increase workers when there are many balanced tasks waiting and executors are busy.

Good signal:

```text
Many tasks
Balanced task durations
Low spill
Low GC
```

## Memory scaling

Increase memory or worker type when tasks spill to disk or GC time is high.

Good signal:

```text
High spill
High GC
Executor memory pressure
OutOfMemory errors
```

## Partition tuning

Use `--SHUFFLE_PARTITIONS` to experiment.

Too few partitions:

```text
Large partitions
Long tasks
Spill/OOM risk
```

Too many partitions:

```text
Many tiny tasks
Scheduler overhead
Too many small output files
```

## Join tuning

Use broadcast joins for small lookup tables.

```python
joined = fact_df.join(broadcast(dim_df), "product_id", "left")
```

Do not broadcast large tables because they can cause memory pressure.

## Cache tuning

Cache only when a DataFrame is reused multiple times.

Bad cache usage:

```text
Cache huge data once and never reuse it
```

Good cache usage:

```text
Cache cleaned/repartitioned dataset reused by quality, join, and aggregation steps
```

## Data quality tuning

Many quality checks can trigger many Spark actions if written as separate `count()` calls. This is acceptable for learning, but in production you may combine checks into fewer passes over data.

## Schema drift tuning

Flexible schema drift handling keeps the pipeline running, but production systems should log and alert when schema drift occurs.
