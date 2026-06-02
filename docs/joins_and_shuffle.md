# Joins and Shuffle

## SQL Join Type vs Spark Join Strategy

SQL join type defines what result you want.

Examples:

- inner join
- left join
- right join
- full join

Spark join strategy defines how Spark physically executes the join.

Examples:

- BroadcastHashJoin
- SortMergeJoin
- ShuffleHashJoin
- Cartesian/Nested Loop Join

## Broadcast Join

Used when one table is small.

Spark copies the small table to every executor and joins locally.

```python
orders.join(broadcast(products), "product_id", "left")
```

## Sort Merge Join

Used when both tables are large.

Spark:

1. shuffles both sides by join key
2. sorts partitions
3. merges matching keys

## Shuffle Hash Join

Spark shuffles data by join key and builds hash maps for matching.

## Cartesian Join

Every row from one side is compared with every row from the other side.

This is expensive and usually avoided.

## Shuffle

Shuffle means data moves across executors so that matching keys are together.

Example:

```sql
orders JOIN customers ON orders.customer_id = customers.customer_id
```

Spark hashes the join key:

```text
partition = hash(customer_id) % number_of_partitions
```

Rows with the same key go to the same partition.
