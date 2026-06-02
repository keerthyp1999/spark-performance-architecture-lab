# Spark Project Tradeoffs

This project intentionally documents tradeoffs because real data engineering is not only about writing code. It is about choosing the right design for scale, cost, quality, and reliability.

## Repartition vs no repartition

| Option | Benefit | Cost |
|---|---|---|
| No repartition | Avoids extra shuffle | May cause poor parallelism or skew |
| Repartition | Better balance and parallelism | Full shuffle cost |

Use repartition when the future operation benefits from better distribution.

## More workers vs more memory

| Option | Benefit | Cost |
|---|---|---|
| More workers | More parallel task execution | Higher cost, no help for skewed single task |
| More memory | Less spill/GC, better for heavy joins/cache | Higher cost, may not improve parallelism |

## Broadcast join vs shuffle join

| Option | Benefit | Risk |
|---|---|---|
| Broadcast join | Avoids large shuffle | Driver/executor memory pressure if table is not small |
| Sort merge join | Scales for large tables | Expensive shuffle and sort |

## Cache vs recompute

| Option | Benefit | Risk |
|---|---|---|
| Cache | Faster reuse of same DataFrame | Memory pressure and GC |
| Recompute | Saves memory | Repeated computation cost |

## Fail-fast vs quarantine

| Option | Benefit | Risk |
|---|---|---|
| Fail-fast | Prevents bad downstream data | Pipeline may stop often |
| Quarantine | Keeps good data flowing | Requires monitoring and remediation |

## Strict schema vs flexible schema drift handling

| Option | Benefit | Risk |
|---|---|---|
| Strict schema | Safer downstream contracts | Frequent failures when source changes |
| Flexible schema | More resilient pipeline | May hide source issues if not monitored |

## Higher shuffle partitions vs lower shuffle partitions

| Option | Benefit | Risk |
|---|---|---|
| Higher partitions | Smaller task memory, more parallelism | More task scheduling overhead, small files |
| Lower partitions | Fewer tasks, less overhead | Larger partitions, spill/OOM risk |

## Cost vs performance

Increasing workers or memory can reduce runtime but increase cost. Good tuning means improving runtime only when the business value justifies the extra compute cost.
