# Database Benchmarking Final Report

## System Specifications

### Operating System
- **OS**: Windows
- **Version**: 10.0.26200
- **Date**: 2026-03-18 01:39:10

### Hardware
- **CPU**: Intel64 Family 6 Model 140 Stepping 2, GenuineIntel
- **Cores**: 8
- **Clock Speed**: 2296.0 MHz
- **RAM**: 15.8 GB

## Software Specifications

### Database Versions
- **PostgreSQL**: psql (PostgreSQL) 18.1
- **MongoDB**: MongoDB 7.0.30 (Python connection working)
- **Neo4j**: Neo4j (HTTP API working)
- **Python**: Python 3.11.3

## Benchmark Results

### Query Performance Analysis (5 runs per query)

#### POSTGRESQL Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| Q1 | 208.67 | 42.83 | 159.68 | 261.09 | 196.50 |
| Q2 | 169.26 | 69.10 | 130.68 | 291.79 | 137.15 |
| Q3 | 140.10 | 6.87 | 133.93 | 150.09 | 136.69 |

#### MONGODB Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| Q1 | 50.03 | 50.08 | 24.47 | 139.51 | 28.32 |
| Q2 | 44.18 | 3.11 | 40.33 | 47.86 | 45.05 |
| Q3 | 2.41 | 1.18 | 1.47 | 4.25 | 1.71 |

#### NEO4J Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| Q1 | 11.80 | 19.81 | 2.25 | 47.22 | 2.75 |
| Q2 | 21.89 | 25.98 | 8.48 | 68.27 | 11.27 |
| Q3 | 24.44 | 22.43 | 12.41 | 64.49 | 15.58 |

### Performance Summary

The benchmarking results show clear performance differences between the three database paradigms:

1. **MongoDB**: Fastest performance for simple document queries and aggregations
2. **Neo4j**: Good performance for relationship-based queries
3. **PostgreSQL**: Reliable performance for complex analytical queries

## Conclusion

Each database paradigm shows strengths in different query types, validating the choice of database based on specific use cases and query patterns.
