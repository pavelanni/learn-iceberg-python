#!/usr/bin/env python3
"""
Example: Handling Data Amendments in Iceberg

Demonstrates different patterns for handling changes to historical data:
1. Versioned Records Pattern (recommended for audit trails)
2. Overwrite Pattern (for corrections)
3. Delete + Insert Pattern (for complex changes)
"""

import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (BooleanType, IntegerType, LongType, NestedField,
                             StringType, TimestampType)


def create_sales_table_example():
    """Create a sample sales table to demonstrate amendments"""

    # Get catalog
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

    catalog = load_catalog("default")

    # Create namespace if needed
    try:
        catalog.create_namespace("sales")
    except:
        pass

    # Define schema for sales with versioning support
    schema = Schema(
        NestedField(field_id=1, name="order_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="customer_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="amount", field_type=LongType(), required=True),  # cents
        NestedField(field_id=4, name="order_date", field_type=TimestampType(), required=True),
        NestedField(field_id=5, name="status", field_type=StringType(), required=True),
        NestedField(field_id=6, name="record_version", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="created_at", field_type=TimestampType(), required=True),
        NestedField(field_id=8, name="is_current", field_type=BooleanType(), required=True),
        NestedField(field_id=9, name="change_reason", field_type=StringType(), required=False),
    )

    # Create table
    try:
        table = catalog.create_table(
            identifier="sales.orders",
            schema=schema,
            location=f"file://{warehouse_path}/sales/orders"
        )
        print("✅ Created sales.orders table")
    except:
        table = catalog.load_table("sales.orders")
        print("ℹ️  Loaded existing sales.orders table")

    return catalog, table

def load_initial_sales_data(table):
    """Load initial sales data"""
    print("\n📊 Loading Initial Sales Data")
    print("-" * 30)

    # Sample sales data
    initial_orders = [
        {
            'order_id': 'ORD-001',
            'customer_id': 'CUST-123',
            'amount': 10000,  # $100.00 in cents
            'order_date': datetime(2024, 1, 15, 10, 30),
            'status': 'completed',
            'record_version': 1,
            'created_at': datetime(2024, 1, 15, 10, 30),
            'is_current': True,
            'change_reason': None
        },
        {
            'order_id': 'ORD-002',
            'customer_id': 'CUST-456',
            'amount': 25000,  # $250.00
            'order_date': datetime(2024, 1, 16, 14, 15),
            'status': 'completed',
            'record_version': 1,
            'created_at': datetime(2024, 1, 16, 14, 15),
            'is_current': True,
            'change_reason': None
        },
        {
            'order_id': 'ORD-003',
            'customer_id': 'CUST-789',
            'amount': 5000,   # $50.00
            'order_date': datetime(2024, 1, 17, 9, 45),
            'status': 'completed',
            'record_version': 1,
            'created_at': datetime(2024, 1, 17, 9, 45),
            'is_current': True,
            'change_reason': None
        }
    ]

    df = pd.DataFrame(initial_orders)

    # Convert to Arrow with proper types
    schema = pa.schema([
        pa.field('order_id', pa.string(), nullable=False),
        pa.field('customer_id', pa.string(), nullable=False),
        pa.field('amount', pa.int64(), nullable=False),
        pa.field('order_date', pa.timestamp('us'), nullable=False),
        pa.field('status', pa.string(), nullable=False),
        pa.field('record_version', pa.int32(), nullable=False),
        pa.field('created_at', pa.timestamp('us'), nullable=False),
        pa.field('is_current', pa.bool_(), nullable=False),
        pa.field('change_reason', pa.string(), nullable=True),
    ])

    arrow_table = pa.Table.from_pandas(df, schema=schema)
    table.append(arrow_table)

    print(f"✅ Loaded {len(df)} initial orders")
    return table

def process_sales_amendment(table):
    """Process an amendment to existing sales data"""
    print("\n🔄 Processing Sales Amendment")
    print("-" * 30)

    print("Scenario: Order ORD-001 amount was incorrect")
    print("Original: $100.00 → Amendment: $120.00")
    print("Reason: Customer applied discount code after initial processing")

    # Step 1: Mark old record as not current
    amendment_records = [
        # Original record marked as superseded
        {
            'order_id': 'ORD-001',
            'customer_id': 'CUST-123',
            'amount': 10000,  # Original amount
            'order_date': datetime(2024, 1, 15, 10, 30),
            'status': 'completed',
            'record_version': 1,
            'created_at': datetime(2024, 1, 15, 10, 30),
            'is_current': False,  # 🔄 Changed to False
            'change_reason': 'Superseded by amendment'
        },
        # New record with corrected amount
        {
            'order_id': 'ORD-001',
            'customer_id': 'CUST-123',
            'amount': 12000,  # $120.00 - corrected amount
            'order_date': datetime(2024, 1, 15, 10, 30),  # Same original date
            'status': 'completed',
            'record_version': 2,  # 🔄 Incremented version
            'created_at': datetime(2024, 1, 20, 16, 45),  # When amendment was processed
            'is_current': True,
            'change_reason': 'Amount correction - discount applied'
        }
    ]

    df = pd.DataFrame(amendment_records)

    # Convert to Arrow
    schema = pa.schema([
        pa.field('order_id', pa.string(), nullable=False),
        pa.field('customer_id', pa.string(), nullable=False),
        pa.field('amount', pa.int64(), nullable=False),
        pa.field('order_date', pa.timestamp('us'), nullable=False),
        pa.field('status', pa.string(), nullable=False),
        pa.field('record_version', pa.int32(), nullable=False),
        pa.field('created_at', pa.timestamp('us'), nullable=False),
        pa.field('is_current', pa.bool_(), nullable=False),
        pa.field('change_reason', pa.string(), nullable=True),
    ])

    arrow_table = pa.Table.from_pandas(df, schema=schema)
    table.append(arrow_table)

    print("✅ Amendment processed")
    print("   - Original record marked as is_current=False")
    print("   - New record added with corrected amount")
    print("   - Full audit trail preserved")

    return table

def query_sales_with_amendments(table):
    """Query sales data handling amendments correctly"""
    print("\n" + "="*50)
    print("QUERYING SALES DATA WITH AMENDMENTS")
    print("="*50)

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        conn.execute("SET unsafe_enable_version_guessing = true")

        table_location = table.location()
        conn.execute(f"CREATE VIEW sales AS SELECT * FROM iceberg_scan('{table_location}')")

        queries = [
            ("All records (including superseded)", "SELECT * FROM sales ORDER BY order_id, record_version"),

            ("Current state (business view)", """
             SELECT order_id, customer_id, amount/100.0 as amount_dollars, order_date, status
             FROM sales
             WHERE is_current = true
             ORDER BY order_id
             """),

            ("Audit trail for ORD-001", """
             SELECT
                 order_id,
                 amount/100.0 as amount_dollars,
                 record_version,
                 created_at,
                 is_current,
                 change_reason
             FROM sales
             WHERE order_id = 'ORD-001'
             ORDER BY record_version
             """),

            ("Total sales (current values only)", """
             SELECT
                 COUNT(*) as total_orders,
                 SUM(amount)/100.0 as total_amount_dollars,
                 AVG(amount)/100.0 as avg_order_dollars
             FROM sales
             WHERE is_current = true
             """),

            ("Amendment history", """
             SELECT
                 COUNT(*) as total_amendments
             FROM sales
             WHERE record_version > 1
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
        print(f"Sales querying failed: {e}")
    finally:
        conn.close()

def demonstrate_amendment_patterns():
    """Show different patterns for handling amendments"""
    print("\n" + "="*50)
    print("AMENDMENT PATTERNS")
    print("="*50)

    print("🎯 Pattern 1: Versioned Records (what we just did)")
    print("   ✅ Complete audit trail")
    print("   ✅ Point-in-time accuracy")
    print("   ✅ Regulatory compliance")
    print("   ❌ More complex queries")
    print("   ❌ Storage overhead")
    print()

    print("🎯 Pattern 2: Overwrite with Copy-on-Write")
    print("   ✅ Simple queries (no version logic)")
    print("   ✅ Less storage overhead")
    print("   ❌ Loses audit trail")
    print("   ❌ Can't query historical state")
    print()

    print("🎯 Pattern 3: Delete + Insert")
    print("   ✅ Clean final state")
    print("   ✅ Efficient for bulk corrections")
    print("   ❌ Complex transaction management")
    print("   ❌ Potential for inconsistent state")
    print()

    print("💡 For MinIO + Iceberg:")
    print("   - Versioned records often best for financial/audit data")
    print("   - Copy-on-write good for operational data")
    print("   - MinIO's object storage handles file versioning efficiently")
    print("   - Iceberg's metadata ensures atomic updates")

# Add this to the main function in the original script
def sales_amendment_demo():
    """Complete demonstration of handling sales amendments"""
    print("\n" + "🏪 SALES AMENDMENT DEMONSTRATION")
    print("="*50)

    # Create sales table
    catalog, sales_table = create_sales_table_example()

    # Load initial data
    sales_table = load_initial_sales_data(sales_table)

    # Process amendment
    sales_table = process_sales_amendment(sales_table)

    # Refresh and query
    sales_table = catalog.load_table("sales.orders")
    query_sales_with_amendments(sales_table)

    # Show patterns
    demonstrate_amendment_patterns()

    print("\n🎉 Sales amendment demo complete!")

if __name__ == "__main__":
    sales_amendment_demo()