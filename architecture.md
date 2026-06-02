# Architecture

## End-to-End Flow

```text
AWS Glue Spark Job
        ↓
Synthetic Orders + Products Generated in Spark
        ↓
Raw Orders with intentional issues
        ↓
Schema Drift Handler
        ↓
Data Quality Engine
        ↓
Clean Dataset + Quarantine Dataset
        ↓
Partition Distribution Inspection
        ↓
Repartition by product_id
        ↓
Cache Repartitioned Orders
        ↓
Broadcast Join with Product Lookup
        ↓
Revenue Aggregation
        ↓
Parquet Outputs to S3
        ↓
Spark UI + CloudWatch Analysis
```

## Why synthetic data?

The project does not depend on Aurora, Redshift, Glue Catalog, crawlers, or external source tables. This makes it easy to run repeatedly while learning Spark UI behavior.

## Key outputs

```text
/curated/orders_clean
/quarantine/orders_invalid
/quality/dq_report
/aggregates/revenue_by_country_category
```

## Runtime components

| Component | Responsibility |
|---|---|
| Driver | Builds logical/physical plans, coordinates jobs/stages/tasks |
| Executors | Process partitions, run tasks, perform joins/aggregations |
| Cluster manager / Glue | Allocates workers and resources |
| S3 | Stores output datasets |
| Spark UI | Shows execution metrics |
| CloudWatch | Shows logs and print statements |

## Learning focus

This architecture intentionally includes operations that appear clearly in Spark UI:

- `count()` and `show()` create Spark jobs.
- `repartition()` creates shuffle and changes task count.
- `broadcast()` demonstrates BroadcastHashJoin.
- `groupBy()` demonstrates aggregation and shuffle behavior.
- `cache()` appears in the Storage tab.
- Data quality checks create multiple jobs because each check is an action.
- Schema drift logs appear in CloudWatch.
