# Runbook: How to Run and What Output to Check

This runbook explains exactly how to run the project and how to connect the output back to Spark UI.

## Option A: Run on AWS Glue

### 1. Create S3 output folder

Create or reuse a bucket and choose an output prefix:

```text
s3://YOUR_BUCKET/spark-performance-lab/output/
```

No input data is required. The Glue job generates synthetic data.

### 2. Create Glue job

Recommended first run:

```text
Glue version: 4.0 or 5.0
Job type: Spark
Language: Python 3
Worker type: G.1X
Number of workers: 2
```

Use the script:

```text
src/glue_spark_job.py
```

### 3. Add Glue job parameters

Required:

```text
--OUTPUT_PATH = s3://YOUR_BUCKET/spark-performance-lab/output/
```

Recommended beginner run:

```text
--DATA_SIZE = medium
--SHUFFLE_PARTITIONS = 20
--SKEW_MODE = true
--SCHEMA_DRIFT_MODE = true
--QUALITY_FAIL_FAST = false
```

### 4. Run 1: Baseline

```text
Workers: 2
Worker type: G.1X
DATA_SIZE: medium
SHUFFLE_PARTITIONS: 20
```

Check Spark UI:

```text
Jobs tab       → number of jobs and slow actions
Stages tab     → shuffle read/write and duration
Tasks tab      → partition balance and skew
SQL tab        → BroadcastHashJoin, Exchange, HashAggregate
Executors tab  → GC, task distribution, memory
Storage tab    → cached DataFrame
```

### 5. Run 2: More workers experiment

Change only:

```text
Number of workers: 4
```

Keep parameters the same.

Compare against baseline:

```text
Did stage duration reduce?
Were there enough tasks to use more workers?
Did executor task distribution improve?
```

### 6. Run 3: More memory experiment

Change worker type:

```text
G.1X → G.2X
```

Use this mainly when you see:

```text
High spill
High GC time
Executor OOM
Large joins or cache pressure
```

### 7. Run 4: More partitions experiment

Change:

```text
--SHUFFLE_PARTITIONS = 40
```

Observe:

```text
More tasks
Smaller shuffle read per task
Possibly more scheduling overhead
Different output file count
```

### 8. Run 5: Schema drift off/on

Run once with:

```text
--SCHEMA_DRIFT_MODE = true
```

Then run:

```text
--SCHEMA_DRIFT_MODE = false
```

Compare CloudWatch logs and output schema.

### 9. Run 6: Data quality fail-fast

Run:

```text
--QUALITY_FAIL_FAST = true
```

Expected behavior:

```text
The job raises an exception because generated data intentionally contains bad records.
```

This demonstrates strict quality gates.

## Option B: Run locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Run default local job:

```bash
python src/local_spark_job.py
```

Run with custom size and partitions:

```bash
python src/local_spark_job.py --row-count 500000 --shuffle-partitions 16 --skew-mode true --schema-drift-mode true
```

Output goes to:

```text
output/sample_results/
```

## Output folders

Glue output:

```text
aggregates/revenue_by_country_category
curated/orders_clean
quarantine/orders_invalid
quality/dq_report
```

Local output:

```text
output/sample_results/aggregates
output/sample_results/curated/orders_clean
output/sample_results/quarantine/orders_invalid
output/sample_results/quality/dq_report
```

## What each output means

| Output | Meaning |
|---|---|
| aggregates | Business metric output after joins and aggregations |
| curated | Clean records that passed quality rules |
| quarantine | Bad records kept for audit/debugging |
| quality | Data quality report with failed counts |

## Simple improvement decision process

```text
1. Find slow job in Jobs tab
2. Open stages for that job
3. Find slow stage
4. Open tasks for that stage
5. Check if task sizes/durations are balanced
6. Check shuffle read/write
7. Check spill and GC
8. Decide: partitioning, broadcast, memory, or workers
```

## Quick tuning guide

| Spark UI symptom | First thing to try |
|---|---|
| Many tasks waiting, balanced work | Increase workers |
| High spill | Increase memory or reduce partition size |
| High GC | Reduce cache/object pressure or increase memory carefully |
| One task much slower | Fix skew, do not blindly add workers |
| SortMergeJoin for small lookup | Try broadcast join |
| Too many tiny tasks | Reduce shuffle partitions |
| Too few huge tasks | Increase shuffle partitions |
