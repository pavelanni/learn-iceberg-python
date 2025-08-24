#!/usr/bin/env python3
"""
Step 1: Create our first Iceberg table

This script demonstrates:
- Setting up an Iceberg catalog
- Defining a table schema
- Creating the table
- Understanding the metadata structure
"""

import os

import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (IntegerType, LongType, NestedField, StringType,
                             TimestampType)


def setup_catalog():
    """Initialize the Iceberg catalog"""
    # Ensure warehouse directory exists (using data/warehouse for better organization)
    warehouse_path = os.path.abspath("data/warehouse")
    os.makedirs(warehouse_path, exist_ok=True)

    # Try direct instantiation approach first
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "sql",
                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )
        print("✅ Catalog initialized with SQLite backend (direct)")
        return catalog
    except Exception as e:
        print(f"Direct approach failed: {e}")

        # Fallback: Create config file approach
        config_content = f"""catalog:
  default:
    type: sql
    uri: sqlite:///{warehouse_path}/pyiceberg_catalog.db
    warehouse: file://{warehouse_path}
"""

        with open(".pyiceberg.yaml", "w") as f:
            f.write(config_content)

        # Set environment variable to point to current directory
        os.environ['PYICEBERG_HOME'] = os.getcwd()

        print("Created .pyiceberg.yaml config file")

        # Load the catalog
        catalog = load_catalog("default")
        print("✅ Catalog initialized with SQLite backend (config file)")
        return catalog

def define_schema():
    """Define the schema for our web server logs table"""
    # This schema matches our CSV structure
    schema = Schema(
        NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=True),
        NestedField(field_id=2, name="ip_address", field_type=StringType(), required=True),
        NestedField(field_id=3, name="method", field_type=StringType(), required=True),
        NestedField(field_id=4, name="url", field_type=StringType(), required=True),
        NestedField(field_id=5, name="status_code", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="response_size", field_type=LongType(), required=True),
        NestedField(field_id=7, name="user_agent", field_type=StringType(), required=False),
    )

    print("✅ Schema defined with 7 fields")
    return schema

def create_table(catalog, schema):
    """Create the Iceberg table"""

    # Create a namespace (like a database schema)
    try:
        catalog.create_namespace("web_logs")
        print("✅ Created namespace 'web_logs'")
    except Exception as e:
        print(f"ℹ️  Namespace might already exist: {e}")

    # Create the table
    table_name = "web_logs.access_logs"
    warehouse_path = os.path.abspath("data/warehouse")
    table_location = os.path.join(warehouse_path, "web_logs", "access_logs")

    try:
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            location=f"file://{table_location}"
        )
        print(f"✅ Created table: {table_name}")
        return table
    except Exception as e:
        print(f"ℹ️  Table might already exist, loading existing: {e}")
        return catalog.load_table(table_name)

def inspect_table_metadata(table):
    """Examine the table structure and metadata"""
    print("\n" + "="*50)
    print("TABLE INSPECTION")
    print("="*50)

    print(f"Table location: {table.location()}")
    print(f"Table schema:")
    for field in table.schema().fields:
        required = "required" if field.required else "optional"
        print(f"  - {field.name}: {field.field_type} ({required})")

    print(f"\nMetadata location: {table.metadata_location}")

    # Show current snapshots (should be empty initially)
    snapshots = list(table.snapshots())
    print(f"Current snapshots: {len(snapshots)}")

    return table

def peek_at_sample_data():
    """Quick look at our CSV data to understand what we're working with"""
    print("\n" + "="*50)
    print("SAMPLE DATA PREVIEW")
    print("="*50)

    df = pd.read_csv("logs/access_log_day1.csv")
    print(f"Day 1 data shape: {df.shape}")
    print("\nFirst 3 rows:")
    print(df.head(3).to_string())

    print(f"\nData types in CSV:")
    print(df.dtypes)

def main():
    """Main execution flow"""
    print("🚀 Creating your first Iceberg table!")
    print("-" * 50)

    # Debug: Check what's available
    try:
        from pyiceberg.catalog.sql import SqlCatalog
        print("✅ SqlCatalog is available")
    except ImportError as e:
        print(f"❌ SqlCatalog not available: {e}")
        print("Make sure to install: uv add 'pyiceberg[sql-sqlite]'")
        return

    # Step 1: Setup catalog
    catalog = setup_catalog()

    # Step 2: Define schema
    schema = define_schema()

    # Step 3: Create table
    table = create_table(catalog, schema)

    # Step 4: Inspect the table
    inspect_table_metadata(table)

    # Step 5: Look at sample data
    peek_at_sample_data()

    print("\n🎉 Table creation complete!")
    print("\nNext steps:")
    print("- Run 02_initial_load.py to load data into the table")
    print("- Explore the 'warehouse' directory to see Iceberg's file structure")

    print(f"\n💡 Pro tip: Check out the files created in:")
    print(f"   - data/warehouse/ (data and metadata files)")
    print(f"   - .pyiceberg.yaml (catalog configuration)")
    if os.path.exists("data/warehouse/pyiceberg_catalog.db"):
        print(f"   - data/warehouse/pyiceberg_catalog.db (catalog database)")


if __name__ == "__main__":
    main()