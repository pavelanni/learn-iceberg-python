#!/usr/bin/env python3
"""
Step 2: Load initial data into our Iceberg table

This script demonstrates:
- Loading data from CSV into Iceberg
- Understanding how Iceberg stores data as Parquet files
- Viewing table snapshots and metadata
- Basic querying with DuckDB
"""

import os
from datetime import datetime

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def get_catalog():
    """Load the existing catalog"""
    # Ensure warehouse directory exists
    warehouse_path = os.path.abspath("data/warehouse")

    # Set environment variable to point to current directory
    os.environ['PYICEBERG_HOME'] = os.getcwd()

    # Check if .pyiceberg.yaml exists, if not create it
    if not os.path.exists(".pyiceberg.yaml"):
        config_content = f"""catalog:
  default:
    type: sql
    uri: sqlite:///{warehouse_path}/pyiceberg_catalog.db
    warehouse: file://{warehouse_path}
"""
        with open(".pyiceberg.yaml", "w") as f:
            f.write(config_content)
        print("ℹ️  Created .pyiceberg.yaml config file")

    # Try direct approach first, fallback to config file approach
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "sql",
                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )
        return catalog
    except Exception:
        # Fallback to config file approach
        return load_catalog("default")

def prepare_data(csv_file):
    """Load and prepare CSV data for Iceberg ingestion"""
    print(f"📁 Loading data from {csv_file}")

    # Load CSV
    df = pd.read_csv(csv_file)

    # Convert timestamp column to proper datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure data types match what Iceberg expects
    df['status_code'] = df['status_code'].astype('int32')  # Iceberg expects int, not long
    df['response_size'] = df['response_size'].astype('int64')  # Keep as long

    # Convert to PyArrow Table with explicit schema that matches Iceberg table
    # This ensures field types and nullability match exactly
    schema = pa.schema([
        pa.field('timestamp', pa.timestamp('us'), nullable=False),  # required timestamp
        pa.field('ip_address', pa.string(), nullable=False),        # required string
        pa.field('method', pa.string(), nullable=False),            # required string
        pa.field('url', pa.string(), nullable=False),               # required string
        pa.field('status_code', pa.int32(), nullable=False),        # required int
        pa.field('response_size', pa.int64(), nullable=False),      # required long
        pa.field('user_agent', pa.string(), nullable=True),         # optional string
    ])

    # Create PyArrow table with explicit schema
    arrow_table = pa.Table.from_pandas(df, schema=schema)

    print(f"✅ Prepared {len(df)} records")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"   Schema: All required fields marked as non-nullable")

    return arrow_table

def load_data_to_iceberg(table, arrow_table):
    """Insert data into the Iceberg table"""
    print("💾 Loading data into Iceberg table...")

    # This creates a new snapshot
    table.append(arrow_table)

    print("✅ Data loaded successfully!")
    return table

def inspect_table_after_load(table):
    """Examine the table after data is loaded"""
    print("\n" + "="*50)
    print("TABLE AFTER LOADING DATA")
    print("="*50)

    # Get current snapshot info
    current_snapshot = table.current_snapshot()
    if current_snapshot:
        print(f"Current snapshot ID: {current_snapshot.snapshot_id}")
        print(f"Timestamp: {datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000)}")
        print(f"Summary: {current_snapshot.summary}")

    # List all snapshots
    snapshots = list(table.snapshots())
    print(f"\nTotal snapshots: {len(snapshots)}")

    # Show data files - using updated API
    try:
        print(f"\nData files in current snapshot:")
        scan = table.scan()

        # Try the newer API first
        if hasattr(scan, 'plan_files'):
            files = list(scan.plan_files())
            print(f"Number of data files: {len(files)}")

            for i, file_info in enumerate(files[:3]):  # Show first 3 files
                print(f"  File {i+1}: {file_info.file_path.split('/')[-1]}")
                print(f"    Records: {file_info.record_count}")
                print(f"    Size: {file_info.file_size_in_bytes} bytes")
        else:
            # Fallback to basic info
            to_arrow_result = scan.to_arrow()
            print(f"Successfully scanned table with {len(to_arrow_result)} records")

    except Exception as e:
        print(f"Could not inspect data files (this is OK): {e}")
        print("Table data is still loaded successfully!")

def query_data_with_duckdb(table):
    """Query the Iceberg table using DuckDB"""
    print("\n" + "="*50)
    print("QUERYING DATA WITH DUCKDB")
    print("="*50)

    # Connect to DuckDB
    conn = duckdb.connect()

    try:
        # Install and load iceberg extension
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")

        # Enable version guessing for local development (as suggested in error)
        conn.execute("SET unsafe_enable_version_guessing = true")

        # Get table location
        table_location = table.location()
        print(f"Reading Iceberg table from: {table_location}")

        # Register the table with DuckDB
        conn.execute(f"""
            CREATE VIEW access_logs AS
            SELECT * FROM iceberg_scan('{table_location}')
        """)

        # Run some basic queries
        queries = [
            ("Total records", "SELECT COUNT(*) as total_records FROM access_logs"),
            ("Status code distribution", """
                SELECT status_code, COUNT(*) as count
                FROM access_logs
                GROUP BY status_code
                ORDER BY count DESC
            """),
            ("Top 5 URLs", """
                SELECT url, COUNT(*) as requests
                FROM access_logs
                GROUP BY url
                ORDER BY requests DESC
                LIMIT 5
            """),
            ("Requests by hour", """
                SELECT
                    EXTRACT(hour FROM timestamp) as hour,
                    COUNT(*) as requests
                FROM access_logs
                GROUP BY hour
                ORDER BY hour
            """)
        ]

        for title, query in queries:
            print(f"\n📊 {title}:")
            print("-" * len(title))
            try:
                result = conn.execute(query).fetchdf()
                print(result.to_string(index=False))
            except Exception as e:
                print(f"Query failed: {e}")

    except Exception as e:
        print(f"DuckDB integration failed: {e}")
        print("This is OK - we can still work with Iceberg directly!")

        # Fallback: Query using PyIceberg directly
        print(f"\n📊 Fallback: Direct PyIceberg queries:")
        print("-" * 40)

        try:
            # Get data as PyArrow table
            arrow_data = table.scan().to_arrow()
            df = arrow_data.to_pandas()

            print(f"Total records: {len(df)}")
            print(f"\nStatus code distribution:")
            print(df['status_code'].value_counts().sort_values(ascending=False))

            print(f"\nTop 5 URLs:")
            print(df['url'].value_counts().head(5))

        except Exception as e2:
            print(f"Direct query also failed: {e2}")

    finally:
        conn.close()

def explore_file_structure():
    """Show what Iceberg created on disk"""
    print("\n" + "="*50)
    print("FILE STRUCTURE EXPLORATION")
    print("="*50)

    def show_directory_tree(path, prefix="", max_depth=3, current_depth=0):
        if current_depth > max_depth:
            return

        items = sorted(os.listdir(path))
        for i, item in enumerate(items):
            item_path = os.path.join(path, item)
            is_last = i == len(items) - 1
            current_prefix = "└── " if is_last else "├── "

            if os.path.isdir(item_path):
                print(f"{prefix}{current_prefix}{item}/")
                next_prefix = prefix + ("    " if is_last else "│   ")
                show_directory_tree(item_path, next_prefix, max_depth, current_depth + 1)
            else:
                size = os.path.getsize(item_path)
                print(f"{prefix}{current_prefix}{item} ({size} bytes)")

    print("data/")
    if os.path.exists("data"):
        show_directory_tree("data")

def main():
    """Main execution flow"""
    print("📊 Loading initial data into Iceberg table")
    print("-" * 50)

    # Load catalog and table
    catalog = get_catalog()
    table = catalog.load_table("web_logs.access_logs")

    # Load and prepare data
    arrow_table = prepare_data("logs/access_log_day1.csv")

    # Load data into Iceberg
    table = load_data_to_iceberg(table, arrow_table)

    # Refresh table metadata
    table = catalog.load_table("web_logs.access_logs")

    # Inspect table after loading
    inspect_table_after_load(table)

    # Query the data
    query_data_with_duckdb(table)

    # Show file structure
    explore_file_structure()

    print("\n🎉 Initial data load complete!")
    print("\nWhat happened:")
    print("- CSV data was converted to Parquet format")
    print("- Iceberg created metadata files to track the data")
    print("- A snapshot was created representing this point in time")
    print("- You can now query this data with any Iceberg-compatible engine")

    print("\nNext steps:")
    print("- Run 03_schema_evolution.py to see how schemas can evolve")
    print("- Try modifying the DuckDB queries above to explore your data")

if __name__ == "__main__":
    main()