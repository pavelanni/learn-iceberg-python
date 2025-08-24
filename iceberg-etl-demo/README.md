# Apache Iceberg Learning Guide

A hands-on introduction to Apache Iceberg using Python, designed for MinIO engineers and data professionals.

## What is Apache Iceberg?

Apache Iceberg is a **table format** for large analytical datasets. Think of it as a specification
for organizing collections of files in object storage to behave like a proper database table with ACID
transactions, schema evolution, and time travel capabilities.

### Key Analogy: Docker/OCI Container Images

Iceberg's architecture is remarkably similar to container images:

```
Docker Container Image          │  Iceberg Table
├── manifest.json (metadata)    │  ├── metadata.json (table schema, snapshots)
├── config.json (configuration) │  ├── version-hint.text (current version)
└── layers/ (tar.gz files)      │  └── data/ (parquet files)
```

Both use:
- **Layered architecture** with file reuse
- **Immutable artifacts** (files never change)
- **Metadata-driven assembly**
- **Content addressing** for integrity
- **Incremental updates** without rebuilding everything

## Iceberg vs Parquet Relationship

**Common Confusion**: Are Parquet and Iceberg competing formats?

**Reality**: They work at different layers:
- **Parquet** = File format (how individual files store data efficiently)
- **Iceberg** = Table format (how collections of files are organized into logical tables)

```
Iceberg Table
├── metadata/ (JSON files tracking schema, partitions, snapshots)
└── data/
    ├── file1.parquet  ← Parquet handles efficient columnar storage
    ├── file2.parquet  ← Parquet handles efficient columnar storage
    └── file3.parquet  ← Parquet handles efficient columnar storage
```

**Analogy**: Parquet is like individual books, Iceberg is the library catalog system.

## Project Setup

### Prerequisites

**System Requirements**:
- Python 3.12+
- uv package manager (install via `curl -LsSf https://astral.sh/uv/install.sh | sh`)

**Common Setup Issues**:
- **pyarrow Version Conflicts**: If you get import errors, ensure compatible versions:
  ```bash
  uv add "pyiceberg[sql-sqlite,duckdb]>=0.9.0" "pyarrow>=17.0.0,<20.0.0"
  ```
- **DuckDB Extension Errors**: Enable version guessing if needed:
  ```python
  conn.execute("SET unsafe_enable_version_guessing = true")
  ```

```bash
# Initialize project with uv (fast Python package manager)
uv init iceberg-etl-demo
cd iceberg-etl-demo

# Add dependencies with compatible versions
uv add "pyiceberg[sql-sqlite,duckdb]>=0.9.0" pandas "pyarrow>=17.0.0,<20.0.0" duckdb

# Generate sample data first
uv run src/generate_logs.py
```

### Project Structure

```
iceberg-etl-demo/
├── data/
│   ├── warehouse/          # Iceberg data and metadata files
│   └── metadata/           # Available for future use
├── logs/
│   ├── access_log_day1.csv # Sample web server logs
│   ├── access_log_day2.csv
│   └── access_log_day3.csv
├── src/
│   ├── 01_create_table.py
│   ├── 02_initial_load.py
│   ├── 03_schema_evolution.py
│   ├── 04_incremental_updates.py
│   └── 05_time_travel_queries.py
├── generate_logs.py        # Generate sample data
└── .pyiceberg.yaml        # Catalog configuration
```

### Understanding the Generated Files

After running the scripts, you'll see this structure:

```
data/warehouse/web_logs/access_logs/
├── data/                           # Immutable Parquet data files
│   ├── 00000-0-<uuid>.parquet     # First data file
│   └── 00001-0-<uuid>.parquet     # Second data file (from append)
├── metadata/                       # Table metadata and history
│   ├── 00000-<uuid>.metadata.json # Initial table metadata
│   ├── 00001-<uuid>.metadata.json # After first data load
│   ├── <uuid>-m0.avro             # Manifest files (track data files)
│   └── snap-<id>-0-<uuid>.avro    # Snapshot manifests
```

**File Types Explained**:
- **`.parquet`** - Your actual data in columnar format
- **`.metadata.json`** - Table schema, partitioning, snapshot history
- **`-m0.avro`** - Manifest files listing which data files belong together
- **`snap-*.avro`** - Snapshot manifests (immutable file lists per version)

## Learning Journey

### Step 1: Table Creation (`01_create_table.py`)

**Concepts Learned**:
- **Catalog setup** - How Iceberg organizes tables (like database schemas)
- **Schema definition** - Structured field definitions with unique IDs
- **Metadata files** - JSON files that track table structure

**Key Insight**: Unlike CSV files, Iceberg schemas have unique field IDs that enable safe evolution.

**What Gets Created**:
```
data/warehouse/
├── pyiceberg_catalog.db (SQLite catalog database)
└── web_logs/
    └── access_logs/
        └── metadata/ (JSON files tracking table structure)
```

### Step 2: Initial Data Load (`02_initial_load.py`)

**Concepts Learned**:
- **CSV to Parquet conversion** - Automatic optimization for analytical queries
- **Snapshots** - Immutable versions of your table (like Git commits)
- **Schema validation** - Strict type checking and nullability enforcement
- **Query integration** - Using DuckDB to query Iceberg tables

**Key Insight**: Your CSV data gets converted to efficient Parquet files automatically, but Iceberg adds
metadata tracking and versioning on top.

**What Happens**:
1. CSV data loaded and validated against schema
1. Data converted to Parquet format for efficiency
1. First snapshot created (snapshot ID assigned)
1. Metadata updated to track the new data files

### Step 3: Schema Evolution (`03_schema_evolution.py`)

**Concepts Learned**:
- **Safe schema changes** - Add fields without breaking existing data
- **Backward compatibility** - Old queries continue to work
- **Field ID assignment** - New fields get unique IDs, never reused
- **Mixed schema queries** - Query across data with different schema versions

**Key Insight**: Schema evolution is **additive and safe**. Old data gets NULL values for new fields automatically.

**Real-World Power**:
```sql
-- This query works even though some records don't have these fields!
SELECT user_country, COUNT(*)
FROM access_logs
WHERE user_country IS NOT NULL  -- Old data will be NULL
GROUP BY user_country
```

**Results We Saw**:
- Schema ID progression: 0 → 1 → 2
- 3000 records with original schema (user_country = NULL)
- 1000 records with enhanced schema (user_country populated)
- All queries work seamlessly across versions

### Step 4: Incremental Updates (`04_incremental_updates.py`)

**Concepts Learned**:
- **Append-only pattern** - Most efficient for time-series data
- **Late-arriving data** - Handle out-of-order data gracefully
- **Atomic operations** - Updates succeed completely or not at all
- **File isolation** - New data creates new files, never modifies existing ones

**Key Insight**: Iceberg makes incremental loading **safe and efficient**. Failed operations can't corrupt
existing data.

**Patterns Demonstrated**:
1. **Simple Append** - Add new data files
1. **Late Data Handling** - Insert historical data that arrived late
1. **Predicate Pushdown** - Efficient filtering during reads

### Step 5: Time Travel Queries (`05_time_travel_queries.py`)

**Concepts Learned**:
- **Snapshot-based versioning** - Every change creates an immutable snapshot
- **Historical queries** - Query table state at any point in time
- **Audit trails** - Complete history of all changes built-in
- **Rollback capabilities** - Safely revert to previous states

**Key Insight**: Time travel isn't just a cool feature - it's **transformational** for data operations,
debugging, and compliance.

**Git Analogy for Data**:
```
Git for Code     │  Iceberg for Data
─────────────────┼──────────────────
git log          │  table.snapshots()
git checkout X   │  table.scan(snapshot_id=X)
git diff A B     │  compare snapshots
git revert       │  rollback to snapshot
git blame        │  audit trail
```

## Troubleshooting Common Issues

### Import Errors
```python
# If you see: ModuleNotFoundError: No module named 'pyiceberg.catalog.sql'
# Solution: Upgrade pyiceberg
uv remove pyiceberg
uv add "pyiceberg[sql-sqlite,duckdb]>=0.9.0"
```

### DuckDB Integration Issues
```python
# If DuckDB can't read Iceberg tables, try:
conn.execute("SET unsafe_enable_version_guessing = true")
# Or use direct PyIceberg scanning:
arrow_data = table.scan().to_arrow()
df = arrow_data.to_pandas()
```

### Performance Tips
```python
# For large datasets:
# 1. Use predicate pushdown
filtered_scan = table.scan(row_filter="status_code == 200")

# 2. Select only needed columns
column_scan = table.scan(selected_fields=["timestamp", "url"])

# 3. Combine both for maximum efficiency
optimized_scan = table.scan(
    row_filter="timestamp >= '2024-01-01'",
    selected_fields=["url", "status_code"]
)
```

## CLI Tools for Data Exploration

Essential command-line tools for working with Parquet files and Iceberg tables:

### **DuckDB CLI (Recommended)** 🦆
```bash
# Install DuckDB CLI
brew install duckdb  # macOS
# or download from duckdb.org

# Query Parquet files directly
duckdb -c "SELECT * FROM 'data/warehouse/web_logs/access_logs/data/*.parquet' LIMIT 10;"
duckdb -c "DESCRIBE SELECT * FROM 'data/warehouse/web_logs/access_logs/data/*.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'data/warehouse/web_logs/access_logs/data/*.parquet';"

# Query Iceberg tables (after running the tutorial)
duckdb
> INSTALL iceberg;
> LOAD iceberg;
> SET unsafe_enable_version_guessing = true;
> SELECT COUNT(*) FROM iceberg_scan('data/warehouse/web_logs/access_logs');
> SELECT status_code, COUNT(*) FROM iceberg_scan('data/warehouse/web_logs/access_logs') GROUP BY status_code;
```

### **PyIceberg CLI** ⚡
```bash
# Built-in with your installation
pyiceberg --help

# List all tables in catalog
pyiceberg list

# Describe table structure and metadata
pyiceberg describe web_logs.access_logs

# Show table schema details
pyiceberg schema web_logs.access_logs

# View table properties
pyiceberg properties web_logs.access_logs

# JSON output for programmatic use
pyiceberg --output json describe web_logs.access_logs
```

### **Parquet Tools** 📊
```bash
# Install parquet inspection tools
uv add parquet-tools

# Inspect Parquet file metadata
parquet-tools show data/warehouse/web_logs/access_logs/data/00000-*.parquet
parquet-tools schema data/warehouse/web_logs/access_logs/data/00000-*.parquet
parquet-tools meta data/warehouse/web_logs/access_logs/data/00000-*.parquet

# Preview data
parquet-tools head data/warehouse/web_logs/access_logs/data/00000-*.parquet --count 5
```

### **Quick Exploration Commands** 🔍

Try these after running the tutorial scripts:

```bash
# 1. Find all Parquet files
find data/warehouse -name "*.parquet" -exec ls -lh {} \;

# 2. Count records across all files
duckdb -c "SELECT COUNT(*) as total_records FROM 'data/warehouse/web_logs/access_logs/data/*.parquet'"

# 3. Analyze file sizes and record counts
duckdb -c "
SELECT
  filename,
  COUNT(*) as records,
  ROUND(SUM(LENGTH(CAST(* AS VARCHAR))) / 1024.0, 2) as approx_kb
FROM 'data/warehouse/web_logs/access_logs/data/*.parquet'
GROUP BY filename
"

# 4. Compare data across snapshots (time travel)
pyiceberg describe web_logs.access_logs | grep -A 5 "snapshots"
```

### **Advanced CLI Patterns** 🚀
```bash
# Join Iceberg table with external CSV
duckdb -c "
SELECT i.status_code, COUNT(*)
FROM iceberg_scan('data/warehouse/web_logs/access_logs') i
JOIN 'logs/access_log_day1.csv' c ON i.ip_address = c.ip_address
GROUP BY i.status_code
"

# Export Iceberg data to different formats
duckdb -c "
COPY (SELECT * FROM iceberg_scan('data/warehouse/web_logs/access_logs') LIMIT 1000)
TO 'export.json' (FORMAT JSON)
"

# Monitor table growth over time
watch -n 5 "pyiceberg describe web_logs.access_logs | grep 'records'"
```

## Key Technical Concepts

### **Snapshots** 📸
- Immutable point-in-time views of your table
- Created on every data modification
- Contain references to data files and metadata
- Enable time travel and rollback capabilities

### **Schema Evolution** 🔄
- Add new fields without downtime or data migration
- Field IDs ensure safe schema changes
- Backward compatibility maintained automatically
- Multiple schema versions coexist in same table

### **Metadata Management** 📋
- JSON files track table structure, partitioning, snapshots
- Atomic updates ensure consistency
- File-level statistics enable query optimization
- Manifest files track which data files belong to which snapshot

### **File Organization** 🗂️
- Data stored as Parquet files for efficiency
- Files are immutable once written
- New operations create new files
- Old files can be cleaned up with retention policies

## Real-World Production Patterns

### Batch ETL Pipeline
```python
def daily_etl_job(date_str):
    """Example production ETL pattern"""
    catalog = load_catalog("production")
    table = catalog.load_table("analytics.web_logs")

    # Process daily data
    daily_data = extract_daily_logs(date_str)
    arrow_table = transform_to_arrow(daily_data)

    # Atomic append
    table.append(arrow_table)
    print(f"✅ Loaded {len(daily_data)} records for {date_str}")
```

### Monitoring and Alerts
```python
def check_table_health(table_name):
    """Monitor table growth and performance"""
    table = catalog.load_table(table_name)
    snapshots = list(table.snapshots())

    latest = snapshots[-1]
    file_count = len(list(table.scan().plan_files()))

    # Alert if too many small files
    if file_count > 1000:
        alert(f"Table {table_name} has {file_count} files - consider compaction")
```

### Data Quality Validation
```python
def validate_data_quality(table, date_partition):
    """Ensure data quality before finalizing"""
    scan = table.scan(row_filter=f"date = '{date_partition}'")
    df = scan.to_arrow().to_pandas()

    # Run quality checks
    assert df['timestamp'].notna().all(), "Missing timestamps"
    assert df['status_code'].between(100, 599).all(), "Invalid status codes"

    return True
```

## Integration with MinIO

Iceberg + MinIO is a powerful combination because:

### **Object Storage Benefits** 📦
- **Scalable storage** - Handle petabytes of data
- **Cost-effective** - Much cheaper than traditional databases
- **Decoupled compute** - Scale storage and compute independently
- **Multi-engine access** - Same data accessible by different tools

### **MinIO-Specific Advantages** ⚡
- **S3 compatibility** - Works with existing S3 tools and workflows
- **High performance** - Optimized for analytical workloads
- **On-premises control** - Keep sensitive data in-house
- **Kubernetes native** - Easy container orchestration

### **Use Cases at MinIO** 🎯
- **Bucket catalog systems** - Track metadata across storage buckets
- **Analytics pipelines** - Process large datasets efficiently
- **Audit logging** - Immutable audit trails for compliance
- **Data lake architectures** - Foundation for modern data platforms

### **MinIO Configuration Example** 🔧

To connect Iceberg to MinIO in production:

```python
# .pyiceberg.yaml for MinIO backend
catalog:
  production:
    type: rest  # or 'sql' for SQLite catalog
    uri: https://your-iceberg-rest-catalog/
    s3.endpoint: https://your-minio-endpoint:9000
    s3.access-key-id: ${MINIO_ACCESS_KEY}
    s3.secret-access-key: ${MINIO_SECRET_KEY}
    s3.region: us-east-1
    warehouse: s3://your-iceberg-bucket/warehouse/
```

```python
# Python code using MinIO backend
import os
from pyiceberg.catalog import load_catalog

os.environ['MINIO_ACCESS_KEY'] = 'your-access-key'
os.environ['MINIO_SECRET_KEY'] = 'your-secret-key'

catalog = load_catalog("production")
table = catalog.load_table("analytics.web_logs")
```

**MinIO Deployment Considerations**:
- **Bucket versioning**: Enable for metadata consistency
- **Lifecycle policies**: Manage old snapshots and orphaned files
- **Access policies**: Separate read/write permissions for different services
- **Multi-part uploads**: Enable for large Parquet files

## Why DuckDB Integration Matters

DuckDB has become the "SQLite for analytics" and pairs perfectly with Iceberg:

### **Local Development** 💻
- **Zero setup** - No database servers to configure
- **Instant queries** - Analyze GB-TB datasets on your laptop
- **Python integration** - Works seamlessly with pandas/PyArrow

### **Performance Characteristics** 🚀
- **Columnar execution** - Optimized for analytical queries
- **Vectorized processing** - Handles large datasets efficiently
- **Multi-threaded** - Uses all available CPU cores
- **Smart I/O** - Only reads relevant data files

### **Scalability Sweet Spot** 📊
- **File-level processing** - Can handle individual files of TB size
- **Query selectivity** - For PB-scale tables, works great with good partitioning
- **Right-sized tool** - Not everything needs Spark or Snowflake

## Handling Data Amendments

For scenarios where historical data needs to change (like sales order corrections):

### **Versioned Records Pattern** (Recommended for Audit Data)
```sql
-- Original order
INSERT INTO orders VALUES (order_id=123, amount=100, version=1, is_current=true)

-- Amendment
INSERT INTO orders VALUES (order_id=123, amount=120, version=2, is_current=true)
UPDATE orders SET is_current=false WHERE order_id=123 AND version=1
```

### **Benefits**
- ✅ Complete audit trail preserved
- ✅ Regulatory compliance friendly
- ✅ Can query state at any point in time
- ✅ Supports complex amendment scenarios

### **Query Patterns**

```sql
-- Current state (business view)
SELECT * FROM orders WHERE is_current = true

-- Historical state
SELECT * FROM orders WHERE created_at <= '2024-01-18'

-- Amendment history
SELECT * FROM orders WHERE order_id = 'ORD-001' ORDER BY version
```

## Best Practices Learned

### **Schema Design** 📐
- Use **unique field IDs** for safe evolution
- Plan for **optional fields** in future evolution
- Choose **appropriate data types** (int32 vs int64, timestamp precision)

### **Incremental Loading** 📈
- **Append-only** for time-series data (logs, events, sensors)
- **Larger batches** = fewer files = better query performance
- **Handle late data** gracefully with proper timestamping

### **Query Optimization** 🎯
- Use **predicate pushdown** to filter at file level
- Leverage **partitioning** for time-based queries
- **Time travel** for investigation and compliance

### **Data Amendments** 🔄
- **Versioned records** for audit trails
- **Atomic operations** prevent partial updates
- **Immutable storage** with mutable semantics

## Next Steps

### **For Production Use**
1. **Partitioning strategies** - Optimize for your query patterns
1. **Retention policies** - Manage snapshot and file lifecycle
1. **MinIO integration** - Deploy with object storage backend
1. **Multi-engine access** - Spark, Trino, DuckDB on same tables

### **Advanced Topics**
1. **Partition evolution** - Change partitioning without data migration
1. **Branch and tag** - Git-like operations for data
1. **Streaming integration** - Real-time data ingestion
1. **Performance tuning** - File sizing, compaction strategies

### **MinIO-Specific Integration**
1. **S3-compatible configuration** - Connect Iceberg to MinIO buckets
1. **Distributed deployments** - Multi-node MinIO clusters
1. **Security integration** - Access controls and encryption
1. **Monitoring and observability** - Track table and query metrics

## Why This Matters for MinIO

Iceberg represents the **future of data lake architectures**:

- **Open format** - No vendor lock-in, works with any storage
- **Cloud-native** - Designed for object storage systems like MinIO
- **Multi-engine** - One table format, many query engines
- **Enterprise features** - ACID transactions, schema evolution, time travel
- **Performance** - Columnar storage with smart metadata

For MinIO, Iceberg enables:
- **Modern data platforms** built on object storage
- **Analytical workloads** that scale to petabytes
- **Compliance and audit** capabilities for enterprise customers
- **Ecosystem integration** with popular data tools

## Advanced Exploration Ideas

Once you've completed the tutorial, try these extensions:

### **Partitioning Experiments** 🗂️
```python
# Create partitioned table for better query performance
partitioned_schema = Schema(
    # ... your fields ...
    NestedField(field_id=8, name="date_partition", field_type=StringType(), required=True)
)

# Partition by date for time-based queries
table = catalog.create_table(
    identifier="web_logs.partitioned_logs",
    schema=partitioned_schema,
    partition_spec=PartitionSpec(
        PartitionField(source_id=8, field_id=1000, transform=IdentityTransform(), name="date_partition")
    )
)
```

### **Multi-Engine Queries** 🔄
```python
# Compare query engines on same Iceberg table
import time

def benchmark_engines(table_location):
    # DuckDB
    start = time.time()
    duckdb_result = duckdb.execute(f"SELECT COUNT(*) FROM iceberg_scan('{table_location}')").fetchone()[0]
    duckdb_time = time.time() - start

    # PyIceberg direct
    start = time.time()
    pyiceberg_result = len(table.scan().to_arrow())
    pyiceberg_time = time.time() - start

    print(f"DuckDB: {duckdb_result} rows in {duckdb_time:.2f}s")
    print(f"PyIceberg: {pyiceberg_result} rows in {pyiceberg_time:.2f}s")
```

### **Custom Data Generators** 📊
```python
# Create realistic data with trends and patterns
def generate_realistic_logs(days=30, events_per_day=10000):
    """Generate logs with realistic traffic patterns"""
    # Weekend vs weekday patterns
    # Peak hours (9am-5pm)
    # Error rate spikes
    # Seasonal trends
    pass
```

## Resources for Further Learning

### **Official Documentation**
- **Apache Iceberg Documentation**: https://iceberg.apache.org/
- **PyIceberg Documentation**: https://py.iceberg.apache.org/
- **DuckDB Documentation**: https://duckdb.org/
- **MinIO Documentation**: https://min.io/docs/

### **Community Resources**
- **Iceberg Slack**: Join the Apache Iceberg community
- **MinIO Community**: https://slack.min.io/
- **DuckDB Discussions**: https://github.com/duckdb/duckdb/discussions

### **Advanced Topics**
- **Iceberg REST Catalog**: For production deployments
- **Spark Integration**: Large-scale processing with Apache Spark
- **Trino Integration**: Distributed SQL queries across data lakes

## Key Takeaways

1. **Iceberg makes object storage feel like a database** - ACID properties, schema management, versioning
1. **Time travel is revolutionary** - Instant historical queries, audit trails, rollback capabilities
1. **Schema evolution is safe** - Add fields without downtime or migration
1. **Incremental loading is efficient** - Append-only patterns, atomic operations
1. **Perfect for MinIO** - Leverages object storage strengths while adding database-like features

The combination of **Iceberg + MinIO + DuckDB** represents a modern, flexible, and powerful data stack that
can handle everything from local development to production-scale analytics.

---

_This guide represents a practical introduction to Iceberg concepts. Each script builds on the previous one
to demonstrate core capabilities that are essential for understanding how Iceberg can enhance MinIO-based
data platforms._
