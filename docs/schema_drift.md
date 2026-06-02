# Schema Drift Handling

Schema drift means incoming data structure changes over time.

Examples:

- A new column arrives.
- An expected column is missing.
- A column type changes.
- A field name changes.

## Strategy used in this project

The job defines an expected schema for orders:

```text
order_id
customer_id
product_id
amount
country
```

Then it aligns raw data to that schema.

## Extra columns

If an unexpected column appears, such as:

```text
coupon_code
```

it is dropped from the curated dataset but logged.

## Missing columns

If an expected column is missing, the job adds it as a null column and casts it to the expected type.

## Type changes

Expected columns are cast to the expected type.

Example:

```text
amount → double
product_id → int
```

## Production options

| Strategy | When to use |
|---|---|
| Strict schema | Critical financial/reporting pipelines |
| Add missing columns as null | Backward compatibility |
| Drop unexpected columns | Curated stable output required |
| Store raw data separately | Audit and replay support |
| Alert on drift | Production monitoring |

## Tradeoff

Strict handling protects downstream consumers but can fail jobs frequently. Flexible handling keeps jobs running but requires strong monitoring.
