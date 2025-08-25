# Iceberg MinIO Integration Demo

Learn how to use Apache Iceberg with MinIO object storage for production-ready data lake architectures. This tutorial demonstrates both local development patterns and production deployment considerations.

> **Prerequisites**: Complete the [ETL Demo](../iceberg-etl-demo/) first to understand Iceberg fundamentals.

## What You'll Learn

- **S3-compatible configuration** for PyIceberg + MinIO
- **Local vs production patterns** for development workflow
- **Bucket management** and security considerations  
- **Performance characteristics** of object storage vs local filesystem
- **Catalog deployment options** (SQL vs REST)
- **Production best practices** for credentials and monitoring

## Project Setup

### Prerequisites

**Option 1 - Local MinIO Instance**:
- MinIO running in Podman/Docker container
- Admin access to create buckets and manage credentials
- Network access to MinIO API (typically localhost:9000)

**Option 2 - MinIO Playground** (No local setup required):
- Uses the public MinIO playground server
- No installation needed, perfect for learning
- Data is temporary - only for testing and tutorials

**System Requirements**:
- Python 3.12+
- uv package manager
- Either local MinIO instance OR internet access for playground

### Installation

**Option A: Local MinIO Setup**
```bash
cd iceberg-minio-demo
uv sync

# Set up environment configuration
cp .env.example .env
# Edit .env with your MinIO credentials (if different from defaults)

# Test MinIO connectivity first
uv run src/01_minio_connection.py
```

**Option B: MinIO Playground Setup** 
```bash
cd iceberg-minio-demo
uv sync

# Set up playground environment
uv run src/00_playground_setup.py
# This creates a unique bucket and .env.playground file

# Copy playground config to .env
cp .env.playground .env
```

**Continue with Tutorial** (both options)
```bash
# Proceed through the tutorial
uv run src/02_catalog_setup.py
uv run src/03_basic_operations.py
uv run src/04_production_patterns.py

# Advanced features (optional)
uv run src/05_bucket_lifecycle_demo.py
uv run src/06_multi_user_security_demo.py  
uv run src/07_backup_disaster_recovery_demo.py
uv run src/08_performance_optimization_demo.py
uv run src/09_monitoring_observability_demo.py
```

### Environment Configuration

The project uses `.env` files for configuration management - **never hardcode credentials!**

```bash
# Copy the example file
cp .env.example .env

# Edit with your settings
vim .env  # or your preferred editor
```

**Important**: The `.env` file is excluded from version control and should never be committed.

### MinIO Playground vs Local Development

| Feature | MinIO Playground | Local MinIO |
|---------|------------------|-------------|
| **Setup** | Zero setup, runs immediately | Requires local container setup |
| **Data persistence** | ❌ Temporary, cleaned up regularly | ✅ Persistent on your machine |
| **Performance** | ⚠️ Network latency, rate limits | ✅ Local network speeds |
| **Privacy** | ❌ Shared environment | ✅ Private, isolated environment |
| **Best for** | Learning, demos, CI/CD testing | Development, debugging, production patterns |

**Recommendation**: 
- **Start with playground** for quick learning and concept understanding
- **Move to local** when you want to experiment with larger datasets or production patterns

## Learning Sequence

### Step 1: MinIO Connection (`01_minio_connection.py`)

**What You'll Learn**:
- Test connectivity to your local MinIO instance
- Understand S3-compatible API authentication
- Create and configure buckets for Iceberg
- Verify read/write permissions

**Key Concepts**:
- **S3 Compatibility** - MinIO implements the S3 API
- **Bucket Organization** - Separating data and metadata
- **Access Patterns** - How Iceberg interacts with object storage

### Step 2: Catalog Setup (`02_catalog_setup.py`)

**What You'll Learn**:  
- Configure PyIceberg to use MinIO as warehouse storage
- Compare SQL vs REST catalog options
- Set up proper warehouse and metadata locations
- Handle connection errors and debugging

**Key Concepts**:
- **Warehouse Location** - Where table data lives in object storage
- **Catalog Backend** - Metadata management (SQLite for dev, REST for prod)
- **Configuration Patterns** - Environment-based settings

### Step 3: Basic Operations (`03_basic_operations.py`)

**What You'll Learn**:
- Create Iceberg tables backed by MinIO storage
- Load data and observe file organization in buckets
- Query data and understand performance characteristics
- Compare with local filesystem performance

**Key Concepts**:
- **File Organization** - How Iceberg organizes data in object storage
- **Network Overhead** - Latency considerations for object storage
- **Metadata Caching** - Why local catalog matters for performance

### Step 4: Production Patterns (`04_production_patterns.py`)

**What You'll Learn**:
- Secure credential management (no hardcoded secrets)
- Multi-user access patterns and bucket policies
- Error handling and retry strategies
- Monitoring and logging patterns

**Key Concepts**:
- **Security** - IAM policies, temporary credentials, encryption
- **Reliability** - Network failures, retry logic, consistency
- **Observability** - Logging, metrics, distributed tracing

## Advanced Features

### Step 5: Bucket Versioning & Lifecycle (`05_bucket_lifecycle_demo.py`)

**What You'll Learn**:
- MinIO bucket versioning for data protection
- Object lifecycle management policies for cost optimization
- Handling corrupted data with versioning recovery
- Integration between Iceberg snapshots and MinIO versioning
- Backup and recovery strategies

**Key Concepts**:
- **Dual Versioning** - Iceberg snapshots + MinIO object versions
- **Lifecycle Policies** - Automatic data tiering and cleanup
- **Corruption Recovery** - Point-in-time recovery from versions
- **Cost Optimization** - Intelligent storage tiering

### Step 6: Multi-User Security (`06_multi_user_security_demo.py`)

**What You'll Learn**:
- IAM user creation and policy management
- Role-based access control (RBAC) patterns
- Temporary credentials and STS workflows
- Audit logging and security monitoring
- Security best practices and implementation

**Key Concepts**:
- **RBAC** - Role-based permissions for data scientists, engineers, admins
- **Temporary Credentials** - Time-limited access with automatic expiration
- **Audit Logging** - Comprehensive security event tracking
- **Access Patterns** - Multi-user scenarios and permission management

### Step 7: Backup & Disaster Recovery (`07_backup_disaster_recovery_demo.py`)

**What You'll Learn**:
- Cross-region replication strategies
- Point-in-time recovery using Iceberg snapshots
- Backup validation and integrity checking
- Disaster recovery runbooks and procedures
- Business continuity planning with RTO/RPO objectives

**Key Concepts**:
- **Cross-Region Replication** - Geographic redundancy and failover
- **Point-in-Time Recovery** - Restore to any snapshot in history
- **Backup Validation** - Automated integrity checking
- **DR Runbooks** - Step-by-step recovery procedures

### Step 8: Performance Optimization (`08_performance_optimization_demo.py`)

**What You'll Learn**:
- MinIO client configuration and connection pooling
- Parallel operations and concurrency patterns
- Iceberg query optimization (partitioning, projection)
- Caching strategies for metadata and data
- Performance monitoring and profiling techniques

**Key Concepts**:
- **Connection Optimization** - Pool management and retry strategies
- **Parallel Operations** - Multi-threaded data operations
- **Query Optimization** - Partition pruning and column projection
- **Caching** - Local, distributed, and CDN caching patterns

### Step 9: Monitoring & Observability (`09_monitoring_observability_demo.py`)

**What You'll Learn**:
- Comprehensive metrics collection and monitoring
- Structured logging with correlation IDs
- Distributed tracing for data operations
- Intelligent alerting rules and thresholds
- Custom dashboards and visualization

**Key Concepts**:
- **Three Pillars** - Metrics, logs, and traces
- **Structured Logging** - JSON logging with contextual information
- **Distributed Tracing** - Request flow tracking across services
- **Intelligent Alerting** - Actionable alerts with runbook links

## Environment Configuration Examples

### Local Development (.env)
```bash
# Local MinIO instance
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_REGION=us-east-1

# PyIceberg catalog configuration
PYICEBERG_CATALOG__MINIO_LOCAL__TYPE=sql
PYICEBERG_CATALOG__MINIO_LOCAL__URI=sqlite:///catalog.db
PYICEBERG_CATALOG__MINIO_LOCAL__WAREHOUSE=s3://iceberg-warehouse/
```

### Production Setup (.env.production)
```bash
# Production MinIO cluster
MINIO_ENDPOINT=https://minio-prod.company.com
MINIO_ACCESS_KEY=${VAULT_MINIO_ACCESS_KEY}  # From secret management
MINIO_SECRET_KEY=${VAULT_MINIO_SECRET_KEY}  # From secret management  
MINIO_REGION=us-west-2

# Production catalog with REST backend
PYICEBERG_CATALOG__PROD__TYPE=rest
PYICEBERG_CATALOG__PROD__URI=https://iceberg-catalog.company.com
PYICEBERG_CATALOG__PROD__WAREHOUSE=s3://prod-iceberg-warehouse/
```

### Cloud Provider Integration
```bash
# AWS with IAM roles (no explicit credentials needed)
MINIO_ENDPOINT=https://s3.amazonaws.com
MINIO_REGION=us-east-1
# Credentials provided by IAM role

# Azure Blob Storage
MINIO_ENDPOINT=https://account.blob.core.windows.net
MINIO_ACCESS_KEY=${AZURE_STORAGE_ACCOUNT}
MINIO_SECRET_KEY=${AZURE_STORAGE_KEY}
```

## Quick Start with Local MinIO

Don't have MinIO running locally? Here's how to set it up:

### Option 1: Podman/Docker
```bash
# Start MinIO in container
podman run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"

# Access MinIO Console at http://localhost:9001
# API endpoint: http://localhost:9000
```

### Option 2: Docker Compose
```yaml
# docker-compose.yml (provided in project)
version: '3.8'
services:
  minio:
    image: quay.io/minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
```

```bash
docker-compose up -d
```

## Common Issues and Solutions

### Connection Problems
```python
# Test basic connectivity
import boto3
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)
s3.list_buckets()  # Should work without errors
```

### Bucket Permissions
```bash
# Create bucket via MinIO Client (mc)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/iceberg-warehouse
mc policy set public local/iceberg-warehouse  # For development only!
```

### SSL/TLS Issues
```python
# For local development, disable SSL verification
import urllib3
urllib3.disable_warnings()

# In production, always use proper certificates!
```

## Architecture Comparison

### Local Filesystem (ETL Demo)
```
iceberg-etl-demo/
├── data/warehouse/     ← Local filesystem
    ├── metadata/       ← Fast access
    └── table_data/     ← No network latency
```

### MinIO Object Storage (This Demo)  
```
MinIO Bucket: iceberg-warehouse
├── metadata/           ← Network calls for metadata
├── table_data/         ← Network calls for data
└── catalog/           ← Can be local or remote
```

## Performance Considerations

### Local Development Benefits
- **Fast iteration** - No network latency for metadata operations
- **Easy debugging** - Direct file system access
- **Simple setup** - No additional infrastructure

### Object Storage Benefits  
- **Scalability** - Handle petabytes of data
- **Durability** - Built-in replication and backup
- **Multi-access** - Same data from multiple compute engines
- **Cost efficiency** - Much cheaper than block storage

## Production Deployment

When moving to production, consider:

### Security
- **IAM policies** instead of root credentials
- **Temporary credentials** for applications
- **Encryption at rest** and in transit
- **Network security** (VPC, firewalls)

### Performance  
- **Catalog placement** - Co-locate with compute
- **Network bandwidth** - Ensure sufficient throughput
- **Concurrent access** - Handle multiple readers/writers
- **Caching strategies** - Reduce metadata calls

### Operations
- **Monitoring** - Track performance and errors
- **Backup strategies** - Protect against data loss
- **Access logging** - Audit and compliance
- **Cost optimization** - Lifecycle policies

## Complete Learning Path

### Foundation (Required)
Start with these core scripts to understand MinIO + Iceberg basics:

1. **`01_minio_connection.py`** - Test MinIO connectivity and bucket operations
1. **`02_catalog_setup.py`** - Configure PyIceberg with MinIO backend
1. **`03_basic_operations.py`** - Create tables, load data, run queries
1. **`04_production_patterns.py`** - Security, error handling, monitoring

### Advanced Features (Optional)

Once comfortable with basics, explore production-ready features:

1. **`05_bucket_lifecycle_demo.py`** - Versioning, lifecycle policies, corruption recovery
1. **`06_multi_user_security_demo.py`** - RBAC, IAM policies, audit logging
1. **`07_backup_disaster_recovery_demo.py`** - Cross-region replication, DR procedures
1. **`08_performance_optimization_demo.py`** - Connection pooling, caching, query optimization
1. **`09_monitoring_observability_demo.py`** - Metrics, logging, distributed tracing

**Estimated Time**:

- Foundation: ~45 minutes
- Advanced Features: ~2-3 hours
- **Total**: 3-4 hours for complete mastery

## Next Steps

After completing this tutorial:

1. **Streaming Demo** - Real-time data ingestion to MinIO
1. **Analytics Demo** - Multi-engine queries (Spark, Trino, DuckDB)
1. **Production Deployment** - REST catalog, monitoring, security

---

**Ready to start?** → Run `uv run src/01_minio_connection.py` to test your MinIO setup!