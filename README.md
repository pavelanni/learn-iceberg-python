# Learn Apache Iceberg with Python

A comprehensive hands-on learning repository for Apache Iceberg, designed for data engineers and professionals working with modern data lake architectures. Special focus on MinIO object storage integration.

## What is Apache Iceberg?

Apache Iceberg is a **table format** for large analytical datasets. Think of it as a specification for organizing collections of files in object storage to behave like a proper database table with ACID transactions, schema evolution, and time travel capabilities.

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

## Why Object Storage + Iceberg?

The combination of object storage (like MinIO) with Iceberg creates a powerful modern data architecture:

### **Object Storage Advantages** 📦
- **Massive scalability** - Store petabytes cost-effectively
- **Decoupled compute and storage** - Scale independently
- **Multi-engine access** - Same data, different processing engines
- **Cloud-native design** - Works across on-premises and cloud

### **Iceberg Adds Database-Like Features** ⚡
- **ACID transactions** - Atomic, consistent, isolated, durable operations
- **Schema evolution** - Add/modify columns without breaking existing data
- **Time travel** - Query historical versions of your data
- **Snapshot isolation** - Consistent reads even during writes
- **Performance optimizations** - File pruning, predicate pushdown

### **Perfect for MinIO** 🎯
- **S3 compatibility** - Works with existing tools and workflows
- **High performance** - Optimized for analytical workloads  
- **On-premises control** - Keep sensitive data in-house
- **Cost efficiency** - Much cheaper than traditional data warehouses
- **Kubernetes native** - Easy container orchestration

## Core Concepts You'll Learn

### **Snapshots** 📸
Every change to an Iceberg table creates an immutable snapshot - like Git commits for data. This enables:
- **Time travel queries** - Query your data as it existed at any point
- **Rollback capabilities** - Safely revert problematic changes
- **Audit trails** - Complete history of all data modifications

### **Schema Evolution** 🔄
Add new columns or modify existing ones without breaking existing queries:
- **Safe changes** - Old queries continue to work
- **Backward compatibility** - New fields get NULL values in old data
- **No downtime** - Schema changes are instantaneous

### **Metadata Management** 📋
Iceberg tracks everything through JSON metadata files:
- **Table schema** - Field definitions with unique IDs
- **Partition information** - How data is organized
- **File statistics** - Enable query optimization
- **Snapshot history** - Complete change tracking

## Learning Path

This repository contains progressive learning projects:

### 1. **ETL Demo** (`iceberg-etl-demo/`)
**Focus**: Fundamentals of Iceberg table operations
- Create your first Iceberg table
- Load data from CSV to Parquet
- Schema evolution in practice
- Time travel queries
- CLI tools for exploration

**Perfect for**: Understanding core concepts and hands-on practice

### 2. **MinIO Integration** (`iceberg-minio-demo/`) 
**Focus**: Production deployment patterns
- Connect Iceberg to MinIO object storage
- S3-compatible configuration and bucket management
- Local development vs production patterns
- Performance optimization and monitoring

### 3. **Real-time Streaming** (`iceberg-streaming-demo/`) *Coming Soon*
**Focus**: Modern data pipeline architectures
- Stream processing with Iceberg
- Late-arriving data handling
- Exactly-once semantics
- Integration with Kafka/Kinesis

### 4. **Analytics Workbench** (`iceberg-analytics-demo/`) *Coming Soon*
**Focus**: Multi-engine data analysis
- Query same data with DuckDB, Spark, Trino
- Performance comparisons
- Query optimization techniques
- Data visualization integration

## Prerequisites

- **Python 3.12+**
- **uv package manager** (recommended) - `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Basic SQL knowledge** - for querying examples
- **Understanding of data formats** - CSV, JSON, Parquet basics

## Quick Start

```bash
# Clone the repository
git clone https://github.com/your-username/learn-iceberg-python.git
cd learn-iceberg-python

# Start with the ETL demo
cd iceberg-etl-demo
uv sync

# Generate sample data and run first tutorial
uv run src/generate_logs.py
uv run src/01_create_table.py
```

## Why This Matters for Data Engineering

Iceberg represents a **paradigm shift** in how we think about data storage:

### **Traditional Challenges** ❌
- **Expensive data warehouses** with vendor lock-in
- **Complex ETL pipelines** that are hard to debug  
- **Schema migrations** that require downtime
- **No version control** for data changes
- **Difficult multi-engine access** - each tool needs its own copy

### **Iceberg Solutions** ✅
- **Open table format** - works with any storage or compute engine
- **ACID transactions** - reliable, consistent data operations
- **Time travel** - built-in versioning and audit capabilities
- **Schema evolution** - safe, backward-compatible changes
- **Performance optimization** - automatic file pruning and statistics
- **Multi-engine compatibility** - one table, many analysis tools

## Real-World Use Cases

### **Data Lake Modernization** 🏗️
Transform existing data lakes into reliable, ACID-compliant systems without vendor lock-in.

### **Financial Data** 💰  
Handle complex audit requirements with immutable snapshots and complete change history.

### **IoT and Time-Series** 📊
Efficiently manage high-volume sensor data with automatic file organization and query optimization.

### **Data Science Workflows** 🔬
Enable reproducible analysis with time travel queries and schema evolution for changing models.

### **Compliance and Governance** 📋
Meet regulatory requirements with immutable audit trails and point-in-time data reconstruction.

## Community and Resources

### **Official Documentation**
- [Apache Iceberg](https://iceberg.apache.org/) - Official project site
- [PyIceberg](https://py.iceberg.apache.org/) - Python library documentation
- [MinIO Documentation](https://min.io/docs/) - Object storage documentation

### **Community**
- [Apache Iceberg Slack](https://iceberg.apache.org/community/) - Join the community discussions
- [MinIO Community](https://slack.min.io/) - Connect with MinIO users and developers

### **Learning Resources**
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/) - Deep technical details
- [Data Engineering Best Practices](https://iceberg.apache.org/docs/latest/best-practices/) - Production guidance

## Contributing

Found an issue or want to improve the tutorials? Contributions are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-tutorial`)
3. Commit your changes (`git commit -m 'Add amazing tutorial'`)
4. Push to the branch (`git push origin feature/amazing-tutorial`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Start your Iceberg journey** → Begin with [`iceberg-etl-demo/`](iceberg-etl-demo/) for hands-on learning!