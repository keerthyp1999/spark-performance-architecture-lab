# Spark Performance & Architecture Lab

A hands-on PySpark and AWS Glue project to understand Spark internals, distributed execution, worker scaling, memory tuning, data quality, schema drift handling, partitions, shuffles, joins, caching, and Spark UI performance debugging.

## Why This Project Exists

This project is built for learning Spark like a real data engineer, not just writing transformations. It shows how Spark code becomes jobs, stages, tasks, shuffles, joins, executor work, memory usage, data quality decisions, schema evolution handling, and output files.

## Project Goals

1. Build an end-to-end Spark ETL pipeline using synthetic data.
2. Understand Spark architecture from code to execution.
3. Learn how to read Spark UI in AWS Glue.
4. Demonstrate worker scaling vs memory scaling decisions.
5. Practice production-style data quality checks and quarantine handling.
6. Demonstrate schema drift handling for unexpected or missing columns.
7. Document tradeoffs clearly for interview and resume discussion.

## Architecture

```text
Synthetic Raw Data Generation
        ↓
Intentional Data Quality Issues + Schema Drift
        ↓
Schema Alignment
        ↓
Data Quality Checks
        ↓
Clean Dataset + Quarantine Dataset
        ↓
Partition Inspection
        ↓
Repartition by Join Key
        ↓
Cache Repartitioned Data
        ↓
Broadcast Join with Product Lookup
        ↓
Aggregation by Country and Category
        ↓
Write Curated, Quarantine, Quality, and Aggregate Outputs
        ↓
Analyze Spark UI and CloudWatch Logs
```

## Concepts Demonstrated

| Concept | Where It Appears |
|---|---|
| Driver | Spark session, query planning, job coordination |
| Executors | Distributed task execution |
| Workers | Glue worker count controls available parallel compute |
| Executor memory | Used by joins, shuffle, aggregation, cache, sort |
| Memory overhead | Non-heap/Python/native/container memory |
| Partitions | `getNumPartitions()`, `spark_partition_id()` |
| Tasks | Spark UI Stages/Tasks tab |
| Repartition | `orders_clean.repartition(shuffle_partitions, "product_id")` |
| Shuffle | Repartition, groupBy, orderBy |
| Broadcast Join | `broadcast(products)` |
| Cache | `orders_repartitioned.cache()` |
| Storage Memory | Spark UI Storage tab |
| Data Quality | Null, duplicate, invalid, and negative amount checks |
| Quarantine | Invalid records written separately |
| Schema Drift | Extra columns dropped, missing columns added as nulls |
| Tradeoffs | Documented in `docs/tradeoffs.md` |

## Repository Structure

```text
spark-performance-architecture-lab/
│
├── README.md
├── architecture.md
├── spark-ui-analysis.md
├── tuning-notes.md
├── requirements.txt
│
├── src/
│   ├── glue_spark_job.py
│   └── local_spark_job.py
│
├── docs/
│   ├── spark_architecture.md
│   ├── memory_management.md
│   ├── joins_and_shuffle.md
│   ├── partitions_and_repartition.md
│   ├── spark_ui_walkthrough.md
│   ├── scaling_workers_vs_memory.md
│   ├── data_quality.md
│   ├── schema_drift.md
│   └── tradeoffs.md
│
├── screenshots/
│   └── Add Spark UI screenshots here
│
└── output/
    └── sample_results/
```

## Step-by-Step Runbook

Use [`RUNBOOK.md`](RUNBOOK.md) for the full AWS Glue and local run procedure, including baseline, more workers, more memory, more partitions, schema drift, and data quality fail-fast experiments.

## How To Run Locally

Install PySpark:

```bash
pip install -r requirements.txt
```

Run:

```bash
python src/local_spark_job.py
```

Custom local run:

```bash
python src/local_spark_job.py --row-count 500000 --shuffle-partitions 16 --skew-mode true --schema-drift-mode true
```

## How To Run On AWS Glue

Create an AWS Glue Spark job and paste/upload `src/glue_spark_job.py`.

Recommended first run:

```text
Glue version: 4.0 or 5.0
Worker type: G.1X
Number of workers: 2
```

Required Glue job parameter:

```text
--OUTPUT_PATH = s3://your-bucket/spark-performance-lab/output/
```

Optional parameters:

```text
--DATA_SIZE = small | medium | large
--SHUFFLE_PARTITIONS = 20
--SKEW_MODE = true
--SCHEMA_DRIFT_MODE = true
--QUALITY_FAIL_FAST = false
```

## Suggested Learning Experiments

| Experiment | Change | What To Observe |
|---|---|---|
| Baseline | 2 workers, medium data | Jobs, stages, tasks, shuffle read/write |
| More workers | Increase worker count | More parallelism, shorter stage duration if enough tasks exist |
| More memory | Use larger worker type | Less spill/GC for memory-heavy stages |
| More shuffle partitions | Increase `--SHUFFLE_PARTITIONS` | More tasks, smaller partition size, more scheduling overhead |
| Skew on/off | Toggle `--SKEW_MODE` | Task imbalance and skew patterns |
| Quality fail-fast | Set `--QUALITY_FAIL_FAST=true` | Pipeline stops when quality checks fail |
| Schema drift | Toggle `--SCHEMA_DRIFT_MODE` | Extra column handling and curated schema alignment |

## What To Check In Spark UI

| Tab | What To Learn |
|---|---|
| Jobs | Which action created which job |
| Stages | Where shuffle happened and how long stages took |
| Tasks | Partition-level execution, skew, task duration |
| SQL | Physical plans like BroadcastHashJoin, Exchange, HashAggregate |
| Executors | GC time, memory, shuffle read/write, failed tasks |
| Storage | Cached DataFrame memory usage |
| Environment | Spark configuration values |

## Scaling Decision Rules

| Symptom | Try Increasing Workers | Try Increasing Memory |
|---|---|---|
| Many pending tasks and CPU busy | Yes | Maybe no |
| Few huge tasks are slow | No, fix partitioning/skew first | Maybe |
| High spill to disk | Maybe | Yes |
| High GC time | Maybe | Yes, but also reduce object/cache pressure |
| Too many tiny tasks | No | No, reduce partitions |
| One task much slower than others | No | Usually no, handle skew |

