# Spark Architecture

## Simple Mental Model

```text
Driver = Brain
Executors = Workers
Partitions = Units of data
Tasks = Units of work
Cluster Manager = Resource allocator
```

## Driver

The driver creates the Spark application and coordinates everything.

It is responsible for:

- starting SparkSession
- creating logical and physical plans
- working with Catalyst optimizer
- building DAGs
- scheduling stages and tasks
- tracking job progress

## Executors

Executors run the actual distributed work.

They are responsible for:

- processing partitions
- executing tasks
- reading and writing shuffle data
- caching/persisting data
- writing output files

## Job, Stage, Task

| Term | Meaning |
|---|---|
| Job | Triggered by an action like count/show/write |
| Stage | Group of tasks between shuffle boundaries |
| Task | Work done on one partition |

## Spark Execution Flow

```text
PySpark Code
    ↓
Py4J bridge
    ↓
Spark Driver JVM
    ↓
Catalyst Optimizer
    ↓
Physical Plan
    ↓
Stages and Tasks
    ↓
Executors process partitions
```
