# Scaling Workers vs Scaling Memory

Spark performance tuning is not always solved by adding more workers. The right decision depends on the bottleneck visible in Spark UI.

## Increase workers when

Increase workers when the job has enough parallel tasks and executors are busy.

Good signs for more workers:

- Many tasks waiting to run.
- Stage has many partitions/tasks.
- CPU utilization is high.
- No major spill or GC issue.
- Work is evenly distributed across tasks.

Example:

```text
Stage has 2,000 tasks
Only a few executors available
Tasks are balanced
No heavy spill
```

In this case, more workers can finish more tasks in parallel.

## Increase memory when

Increase memory when each task needs more room to process data.

Good signs for more memory:

- High spill to disk.
- High executor GC time.
- OutOfMemory errors.
- Large joins or aggregations.
- Cache does not fit in memory.

Example:

```text
Tasks are spilling to disk heavily
GC time is high
Executor memory is close to full
```

In this case, more workers alone may not help. Each executor needs more memory.

## When neither is the first fix

If one task is much slower than all other tasks, that is usually skew.

Example:

```text
19 tasks finish in 20 seconds
1 task runs for 20 minutes
```

Adding workers may not fix this because one partition is too large. Better options:

- Repartition by a better key.
- Salt skewed keys.
- Enable AQE skew join handling.
- Broadcast a small lookup table.

## Glue-specific thinking

In AWS Glue:

- Number of workers controls how much distributed compute you get.
- Worker type controls CPU and memory per worker.
- G.1X is good for normal learning and moderate workloads.
- Larger workers help when memory pressure, spill, or GC is visible.

## Practical rule

```text
More workers = more parallelism
More memory = larger per-task working room
Better partitioning = better balance
```
