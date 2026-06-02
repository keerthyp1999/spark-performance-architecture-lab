# Spark Memory Management

## Executor Memory

Executor memory is used by worker processes to execute Spark tasks.

```text
Executor Container
│
├── JVM Heap
│   ├── Execution Memory
│   └── Storage Memory
│
├── Memory Overhead
│   ├── Python workers
│   ├── native memory
│   └── JVM overhead
│
├── Off-Heap Memory optional
└── PySpark Memory optional
```

## Execution Memory

Used for:

- joins
- aggregations
- sorts
- shuffle operations

## Storage Memory

Used for:

- cache
- persist
- broadcast variables

## Driver Memory

Driver memory is used for:

- query planning
- SparkContext/SparkSession
- metadata
- DAGs
- collecting results

Avoid bringing huge data to driver using:

```python
df.collect()
df.toPandas()
```

## Garbage Collection

Garbage collection is JVM memory cleanup.

High GC time means Spark is spending too much time cleaning memory instead of processing data.

## Memory Overhead vs Off-Heap

Memory overhead is broad extra container memory outside JVM heap.

Off-heap memory is a specific optional memory area outside JVM heap.

Off-heap is not the same as overhead, but it contributes to total container memory.
