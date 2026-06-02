# Data Quality Checks

This project includes data quality checks to make the Spark job feel closer to real production ETL.

## Checks included

The Glue job validates:

- Null `order_id`
- Null `customer_id`
- Null `product_id`
- Null `amount`
- Negative `amount`
- Invalid `country`
- Duplicate `order_id`

## Why this matters

Bad data can silently corrupt business metrics. For example, negative amounts or duplicate order IDs can affect revenue calculations.

## Clean vs quarantine approach

The job creates two datasets:

```text
orders_clean      → valid records used for analytics
orders_invalid    → bad records written to quarantine
```

This is better than simply dropping bad records without trace.

## Output paths

```text
/output/curated/orders_clean
/output/quarantine/orders_invalid
/output/quality/dq_report
/output/aggregates/revenue_by_country_category
```

## Fail-fast option

If this parameter is set:

```text
--QUALITY_FAIL_FAST=true
```

then the job stops when quality issues are detected.

## Tradeoff

| Approach | Benefit | Risk |
|---|---|---|
| Drop invalid records | Simple | Loss of auditability |
| Quarantine invalid records | Traceable | Extra storage and logic |
| Fail fast | Protects downstream systems | Can block delivery |
| Continue with report | Keeps pipeline running | Requires monitoring discipline |
