# Database Benchmarking Report

## System Specifications

### Operating System
- **OS**: Windows
- **Version**: 10.0.26200
- **Date**: 2026-03-17 07:35:23

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

### Setup Details
- **Virtualization**: None (Native Windows)
- **Containerization**: Not used (Direct installation)
- **Environment**: Local development environment

## Benchmark Results

### Query Performance Analysis (5 runs per query)
#### POSTGRESQL Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| q1 | 101.75 | 3.63 | 97.89 | 105.76 | 101.70 |
| q2 | 98.10 | 0.69 | 97.00 | 98.90 | 98.19 |
| q3 | 97.58 | 0.86 | 96.70 | 98.57 | 97.63 |
#### MONGODB Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| q1 | 24.25 | 7.91 | 15.36 | 31.02 | 28.46 |
| q2 | 24.95 | 9.53 | 16.11 | 35.62 | 19.93 |
| q3 | 29.67 | 16.03 | 14.91 | 47.76 | 24.30 |
#### NEO4J Results

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|
| q1 | 21.97 | 12.18 | 8.51 | 34.36 | 26.74 |
| q2 | 28.07 | 8.38 | 13.42 | 33.56 | 30.91 |
| q3 | 11.58 | 3.37 | 9.62 | 17.55 | 10.00 |

### Performance Summary

| Database | Q1 Mean (ms) | Q2 Mean (ms) | Q3 Mean (ms) | Overall Mean (ms) |
|----------|---------------|---------------|---------------|------------------|
| POSTGRESQL | 101.75 | 98.10 | 97.58 | 99.14 |
| MONGODB | 24.25 | 24.95 | 29.67 | 26.29 |
| NEO4J | 21.97 | 28.07 | 11.58 | 20.54 |

### Performance Analysis

#### Query Types
- **Q1**: Campaign Effectiveness Analysis
- **Q2**: Personalized Product Recommendations  
- **Q3**: Keyword-Based Product Search

#### Database Characteristics
- **PostgreSQL**: Relational database optimized for complex joins and analytics
- **MongoDB**: Document database optimized for flexible data and horizontal scaling
- **Neo4j**: Graph database optimized for relationship traversal and network analysis

#### Expected Performance Patterns
- **PostgreSQL**: Best for analytical queries with complex aggregations
- **MongoDB**: Best for document-based queries and flexible data access
- **Neo4j**: Best for graph traversal and relationship-based queries

---

## Methodology

### Benchmarking Process
1. **System Setup**: Native Windows environment with local database installations
2. **Data Loading**: All databases loaded with identical datasets (6M+ records)
3. **Query Execution**: Each query executed 5 times per database
4. **Performance Measurement**: Execution time measured in milliseconds
5. **Statistical Analysis**: Mean, standard deviation, min, max, median calculated

### Environment Variables
- **POSTGRES_HOST**: localhost
- **POSTGRES_PORT**: 5432
- **POSTGRES_DB**: ecommerce
- **POSTGRES_USER**: ecommerce_user
- **MONGO_HOST**: localhost
- **MONGO_PORT**: 27017
- **MONGO_DB**: ecommerce
- **NEO4J_URI**: bolt://localhost:7687
- **NEO4J_USER**: neo4j

### Data Volume
- **Campaigns**: ~1,900 records
- **Events**: ~1.3M records
- **Friends**: ~2M records
- **Messages**: ~3M records
- **Purchases**: ~174K records

---

## Conclusion

This benchmarking analysis provides comprehensive performance metrics for PostgreSQL, MongoDB, and Neo4j databases in the context of e-commerce data modeling and analysis. The results demonstrate the relative strengths of each database type for different query patterns and use cases.

*Report generated on 2026-03-17 07:35:23*