# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is a comprehensive Apache Iceberg learning repository with multiple hands-on projects. The repository demonstrates key concepts through progressive examples, showing how to use Iceberg with Python for modern data lake architectures.

## Repository structure

```
learn-iceberg-python/
├── README.md                    # Conceptual overview and learning path
├── LICENSE                      # MIT license
├── CLAUDE.md                    # This file
├── iceberg-etl-demo/           # First tutorial - ETL fundamentals
│   ├── README.md               # Step-by-step ETL tutorial
│   ├── src/                    # Learning scripts (01-05 + bonus)
│   ├── data/warehouse/         # Generated Iceberg tables
│   └── logs/                   # Sample CSV data
├── iceberg-minio-demo/         # Future: MinIO integration
├── iceberg-streaming-demo/     # Future: Real-time processing
└── iceberg-analytics-demo/     # Future: Multi-engine queries
```

## Development workflow

### Getting started
```bash
# Start with the ETL demo
cd iceberg-etl-demo
uv sync

# Generate sample data
uv run src/generate_logs.py
```

### Running tutorials
Each demo directory has its own learning sequence. For the ETL demo:

```bash
# Progressive learning scripts
uv run src/01_create_table.py      # Table creation basics
uv run src/02_initial_load.py      # Data loading and snapshots
uv run src/03_schema_evolution.py  # Safe schema changes
uv run src/04_incremental_updates.py # Append operations
uv run src/05_time_travel_queries.py # Historical queries
uv run src/sales_amendment_demo.py   # Business amendment patterns
```

## Architecture notes

### Technology stack
- **Python 3.12+** with uv package manager
- **PyIceberg 0.9.0+** with SQL SQLite catalog
- **DuckDB** for analytical queries
- **PyArrow** for data format conversions
- **Pandas** for data manipulation

### Key concepts demonstrated
- **Table formats**: Iceberg with Parquet data files
- **Catalog systems**: SQLite for development, REST for production
- **Schema evolution**: Backward-compatible column additions
- **Time travel**: Snapshot-based versioning
- **ACID properties**: Atomic operations with rollback capabilities

### File organization patterns
```
iceberg-etl-demo/data/warehouse/web_logs/access_logs/
├── data/           # Parquet data files (immutable)
├── metadata/       # JSON schema and snapshot metadata
    ├── *.metadata.json  # Table metadata versions
    ├── *.avro           # Manifest files tracking data files
    └── snap-*.avro      # Snapshot manifest files
```

## Development patterns

### Dependency management
- Use compatible versions: `pyiceberg>=0.9.0`, `pyarrow>=17.0.0,<20.0.0`
- Avoid version conflicts between PyArrow and PyIceberg
- Pin DuckDB for stable Iceberg integration

### Error handling
- Graceful fallbacks when DuckDB integration fails
- Direct PyIceberg queries as backup for data access
- Clear error messages for common setup issues

### Code organization
- Progressive tutorial structure (01, 02, 03...)
- Comprehensive docstrings explaining concepts
- CLI-friendly scripts with clear output
- Separate data generation from learning scripts