#!/usr/bin/env python3
"""
Step 3: Schema Evolution with Iceberg

This script demonstrates:
- Adding new columns to an existing table
- Loading data with the evolved schema
- Querying across old and new schema versions
- Understanding how Iceberg handles schema changes gracefully
"""

import os
import random
from datetime import datetime

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.types import BooleanType, NestedField, StringType


def get_catalog():
    """Load the existing catalog"""
    warehouse_path = os.path.abspath("data/warehouse")
    os.environ['PYICEBERG_HOME'] = os.getcwd()

    if not os.path.exists(".pyiceberg.yaml"):
        config_content = f"""catalog:
  default:
    type: sql
    uri: sqlite:///{warehouse_path}/pyiceberg_catalog.db
    warehouse: file://{warehouse_path}
"""
        with open(".pyiceberg.yaml", "w") as f:
            f.write(config_content)

    return load_catalog("default")

def show_current_schema(table):
    """Display the current table schema"""
    print("\n" + "="*50)
    print("CURRENT TABLE SCHEMA")
    print("="*50)

    schema = table.schema()
    print(f"Schema ID: {schema.schema_id}")
    print("Fields:")
    for field in schema.fields:
        required = "required" if field.required else "optional"
        print(f"  {field.field_id}: {field.name} ({field.field_type}) - {required}")

    return schema

def evolve_schema_add_fields(table):
    """Add new fields to demonstrate schema evolution"""
    print("\n" + "="*50)
    print("EVOLVING SCHEMA - ADDING NEW FIELDS")
    print("="*50)

    # Check current schema to see what fields already exist
    current_schema = table.schema()
    existing_fields = {field.name for field in current_schema.fields}

    # Define the fields we want to add
    fields_to_add = [
        ("user_country", StringType(), "Country parsed from IP address"),
        ("browser", StringType(), "Browser parsed from user agent"),
        ("is_mobile", BooleanType(), "Whether request came from mobile device")
    ]

    print("Checking which fields need to be added:")

    # Only add fields that don't already exist
    fields_added = []
    with table.update_schema() as update:
        for field_name, field_type, field_doc in fields_to_add:
            if field_name not in existing_fields:
                update.add_column(field_name, field_type, doc=field_doc)
                fields_added.append(field_name)
                print(f"  ✅ Adding: {field_name}")
            else:
                print(f"  ⏭️  Skipping: {field_name} (already exists)")

    if fields_added:
        print(f"✅ Schema evolved! Added {len(fields_added)} new fields: {fields_added}")
    else:
        print("ℹ️  Schema already has all target fields - no changes needed")

    return table

def generate_enhanced_data():
    """Generate day 2 data with the new fields"""
    print("\n" + "="*50)
    print("GENERATING ENHANCED DATA")
    print("="*50)

    # Read the existing day 2 CSV
    if not os.path.exists("logs/access_log_day2.csv"):
        print("❌ logs/access_log_day2.csv not found. Please generate it first.")
        return None

    df = pd.read_csv("logs/access_log_day2.csv")
    print(f"📁 Loaded {len(df)} records from day 2")

    # Add the new fields with some realistic sample data
    countries = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU', 'BR']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Chrome Mobile']

    # Simulate parsing country from IP (in reality, you'd use a geo-IP service)
    df['user_country'] = [random.choice(countries) for _ in range(len(df))]

    # Simulate parsing browser from user agent
    df['browser'] = [random.choice(browsers) for _ in range(len(df))]

    # Simulate mobile detection (mobile if user agent contains "Mobile" or browser is "Chrome Mobile")
    df['is_mobile'] = df.apply(lambda row:
        'Mobile' in str(row['user_agent']) or row['browser'] == 'Chrome Mobile', axis=1)

    print("✅ Enhanced data with new fields:")
    print(f"   - Countries: {df['user_country'].value_counts().to_dict()}")
    print(f"   - Browsers: {df['browser'].value_counts().to_dict()}")
    print(f"   - Mobile users: {df['is_mobile'].sum()}/{len(df)} ({df['is_mobile'].mean():.1%})")

    return df

def prepare_enhanced_data(df):
    """Prepare the enhanced data for Iceberg"""
    print("\n💾 Preparing enhanced data for Iceberg...")

    # Convert timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure proper data types
    df['status_code'] = df['status_code'].astype('int32')
    df['response_size'] = df['response_size'].astype('int64')
    df['is_mobile'] = df['is_mobile'].astype('bool')

    # Create PyArrow schema with new fields
    schema = pa.schema([
        pa.field('timestamp', pa.timestamp('us'), nullable=False),
        pa.field('ip_address', pa.string(), nullable=False),
        pa.field('method', pa.string(), nullable=False),
        pa.field('url', pa.string(), nullable=False),
        pa.field('status_code', pa.int32(), nullable=False),
        pa.field('response_size', pa.int64(), nullable=False),
        pa.field('user_agent', pa.string(), nullable=True),
        pa.field('user_country', pa.string(), nullable=True),  # New field
        pa.field('browser', pa.string(), nullable=True),       # New field
        pa.field('is_mobile', pa.bool_(), nullable=True),      # New field
    ])

    arrow_table = pa.Table.from_pandas(df, schema=schema)
    print("✅ Enhanced data prepared with new schema")

    return arrow_table

def load_enhanced_data(table, arrow_table):
    """Load the enhanced data into the table"""
    print("\n💾 Loading enhanced data into Iceberg table...")

    # This will work even though we added new fields!
    table.append(arrow_table)

    print("✅ Enhanced data loaded successfully!")
    print("   - Old data still accessible")
    print("   - New data has additional fields")
    print("   - Queries work across both versions")

    return table

def query_evolved_data(table):
    """Query data across schema versions"""
    print("\n" + "="*50)
    print("QUERYING ACROSS SCHEMA VERSIONS")
    print("="*50)

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        conn.execute("SET unsafe_enable_version_guessing = true")

        table_location = table.location()
        conn.execute(f"""
            CREATE VIEW access_logs AS
            SELECT * FROM iceberg_scan('{table_location}')
        """)

        queries = [
            ("Total records (all schema versions)",
             "SELECT COUNT(*) as total_records FROM access_logs"),

            ("Records by schema version (checking for new fields)", """
             SELECT
                 CASE
                     WHEN user_country IS NULL THEN 'Original Schema'
                     ELSE 'Enhanced Schema'
                 END as schema_version,
                 COUNT(*) as records
             FROM access_logs
             GROUP BY 1
             """),

            ("Country distribution (new field - only enhanced data)", """
             SELECT user_country, COUNT(*) as requests
             FROM access_logs
             WHERE user_country IS NOT NULL
             GROUP BY user_country
             ORDER BY requests DESC
             """),

            ("Browser analysis (new field)", """
             SELECT browser, COUNT(*) as requests
             FROM access_logs
             WHERE browser IS NOT NULL
             GROUP BY browser
             ORDER BY requests DESC
             """),

            ("Mobile vs Desktop traffic", """
             SELECT
                 CASE
                     WHEN is_mobile IS NULL THEN 'Unknown (Original Data)'
                     WHEN is_mobile THEN 'Mobile'
                     ELSE 'Desktop'
                 END as device_type,
                 COUNT(*) as requests
             FROM access_logs
             GROUP BY 1
             ORDER BY requests DESC
             """),

            ("Status codes across all data", """
             SELECT status_code, COUNT(*) as count
             FROM access_logs
             GROUP BY status_code
             ORDER BY count DESC
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
        print(f"DuckDB querying failed: {e}")

        # Fallback to direct PyIceberg
        print(f"\n📊 Fallback: Direct PyIceberg analysis:")
        print("-" * 40)

        try:
            arrow_data = table.scan().to_arrow()
            df = arrow_data.to_pandas()

            print(f"Total records: {len(df)}")

            # Check for new fields
            has_country = 'user_country' in df.columns
            print(f"Has enhanced fields: {has_country}")

            if has_country:
                print("\nCountry distribution:")
                country_counts = df['user_country'].value_counts()
                print(country_counts)

        except Exception as e2:
            print(f"Direct query failed: {e2}")

    finally:
        conn.close()

def show_schema_history(table):
    """Show the evolution of the table schema"""
    print("\n" + "="*50)
    print("SCHEMA EVOLUTION HISTORY")
    print("="*50)

    # Get all snapshots to see schema evolution
    snapshots = list(table.snapshots())

    print(f"Total snapshots: {len(snapshots)}")
    print("\nSnapshot history:")

    for i, snapshot in enumerate(snapshots):
        timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
        operation = snapshot.summary.get('operation', 'unknown')

        print(f"\n  Snapshot {i+1}:")
        print(f"    ID: {snapshot.snapshot_id}")
        print(f"    Time: {timestamp}")
        print(f"    Operation: {operation}")
        print(f"    Schema ID: {snapshot.schema_id}")

    print(f"\nCurrent schema ID: {table.schema().schema_id}")
    print("✨ Schema evolution allows:")
    print("   - Old queries still work")
    print("   - New fields are nullable for old data")
    print("   - No data migration required")

def main():
    """Main execution flow"""
    print("🔄 Demonstrating Iceberg Schema Evolution")
    print("-" * 50)

    # Load existing table
    catalog = get_catalog()
    table = catalog.load_table("web_logs.access_logs")

    # Show current schema
    show_current_schema(table)

    # Evolve the schema
    table = evolve_schema_add_fields(table)

    # Show new schema
    show_current_schema(table)

    # Generate enhanced data
    enhanced_df = generate_enhanced_data()
    if enhanced_df is None:
        print("Cannot continue without day 2 data. Please run your data generation script.")
        return

    # Prepare and load enhanced data
    enhanced_arrow = prepare_enhanced_data(enhanced_df)
    table = load_enhanced_data(table, enhanced_arrow)

    # Refresh table to see new data
    table = catalog.load_table("web_logs.access_logs")

    # Query the evolved data
    query_evolved_data(table)

    # Show schema history
    show_schema_history(table)

    print("\n🎉 Schema evolution complete!")
    print("\nKey takeaways:")
    print("- ✅ Added new fields without breaking existing data")
    print("- ✅ Old data remains queryable with NULL values for new fields")
    print("- ✅ New data has all fields populated")
    print("- ✅ Same table serves both old and new schema versions")
    print("- ✅ No data migration or downtime required")

    print("\nNext steps:")
    print("- Run 04_incremental_updates.py for incremental data loading")
    print("- Try querying specific snapshots (time travel)")

if __name__ == "__main__":
    main()