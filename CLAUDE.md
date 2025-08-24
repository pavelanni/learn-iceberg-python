# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is a comprehensive Apache Iceberg learning project demonstrating key concepts through progressive examples. The project shows how to use Iceberg with Python for data lake architectures, including table creation, schema evolution, time travel, and data amendments.

## Development setup

```bash
# Install dependencies (using uv package manager)
cd iceberg-etl-demo
uv sync

# Generate sample log data (creates CSV files in logs/)
uv run src/generate_logs.py
```

## Running the learning sequence

Execute scripts in order to learn Iceberg concepts progressively:

```bash
# Step 1: Create Iceberg table with schema
uv run src/01_create_table.py

# Step 2: Load initial data from CSV into Iceberg
uv run src/02_initial_load.py

# Step 3: Demonstrate schema evolution (adding columns)
uv run src/03_schema_evolution.py

# Step 4: Show incremental updates and late-arriving data
uv run src/04_incremental_updates.py

# Step 5: Time travel queries and snapshot management
uv run src/05_time_travel_queries.py

# Bonus: Data amendment patterns for business corrections
uv run src/sales_amendment_demo.py
```

## Project structure

### Core learning scripts
- `src/01_create_table.py` - Sets up Iceberg catalog (SQLite backend) and creates web logs table schema
- `src/02_initial_load.py` - Loads CSV data into Iceberg table, creates snapshots, demonstrates DuckDB querying
- `src/03_schema_evolution.py` - Shows safe schema changes (adding columns) without breaking existing data
- `src/04_incremental_updates.py` - Demonstrates append operations, late-arriving data, and atomic updates
- `src/05_time_travel_queries.py` - Time travel queries, snapshot management, and rollback capabilities
- `src/sales_amendment_demo.py` - Business amendment patterns (versioned records, overwrites, corrections)
- `src/generate_logs.py` - Generates sample web server log data for testing

### Data organization
- `logs/` - Contains generated CSV log files (access_log_day1-3.csv)
- `data/warehouse/` - Iceberg data warehouse directory (created by scripts)
  - `web_logs/access_logs/` - Web logs table with data/ and metadata/ subdirs
  - `sales/orders/` - Sales demo table for amendment examples
  - `pyiceberg_catalog.db` - SQLite catalog database
- `data/metadata/` - Available for additional metadata storage

## Architecture notes

### Iceberg stack
- **Catalog**: SQLite-based catalog for development (configured via `.pyiceberg.yaml`)
- **Table format**: Iceberg with Parquet data files and Avro manifest files
- **Query engine**: DuckDB integration via `iceberg_scan()` function
- **Data flow**: CSV → PyArrow Table → Iceberg Table (stored as Parquet)

### Key concepts demonstrated
- **Snapshots**: Immutable table versions created on each modification
- **Schema evolution**: Safe column additions with backward compatibility
- **Time travel**: Query historical table states via snapshot IDs
- **ACID properties**: Atomic operations with rollback capabilities
- **Metadata management**: JSON metadata files track schema, partitions, and snapshots

### File organization patterns
```
data/warehouse/web_logs/access_logs/
├── data/           # Parquet data files (immutable)
├── metadata/       # JSON schema and snapshot metadata
    ├── *.metadata.json  # Table metadata versions
    ├── *.avro           # Manifest files tracking data files
    └── snap-*.avro      # Snapshot manifest files
```

## Development patterns

### Catalog configuration
- Uses direct instantiation with fallback to `.pyiceberg.yaml` config file
- SQLite catalog suitable for development and learning
- Warehouse path: `data/warehouse/` (organized under data/ directory)

### Error handling
- Graceful fallbacks when DuckDB integration fails
- Direct PyIceberg queries as backup for data access
- Namespace and table existence checks with informative messages

### Data type handling
- Explicit PyArrow schema matching for CSV imports
- Proper nullable field handling (user_agent as optional)
- Type conversions (int32 for status_code, int64 for response_size)