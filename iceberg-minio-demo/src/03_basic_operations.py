#!/usr/bin/env python3
"""
Step 3: Basic Iceberg Operations with MinIO

This script demonstrates:
- Creating Iceberg tables backed by MinIO storage
- Loading sample data into MinIO-backed tables
- Querying data and observing performance
- Understanding file organization in object storage
- Comparing with local filesystem performance
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (IntegerType, LongType, NestedField, StringType,
                             TimestampType)
from rich.console import Console
from rich.table import Table

console = Console()


def generate_sample_data(num_records=1000):
    """Generate sample web server logs for testing"""
    console.print(f"📊 Generating {num_records} sample records...")
    
    import random
    from datetime import datetime, timedelta
    
    # Sample data components
    ips = ['192.168.1.100', '10.0.0.50', '203.0.113.10', '198.51.100.25', '172.16.0.10']
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    urls = ['/api/users', '/api/orders', '/static/css/style.css', '/index.html', '/api/products']
    status_codes = [200, 200, 200, 200, 404, 500, 201, 204]  # Weighted toward 200
    
    base_date = datetime(2024, 1, 15)
    
    logs = []
    for i in range(num_records):
        timestamp = base_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        log_entry = {
            'timestamp': timestamp,
            'ip_address': random.choice(ips),
            'method': random.choice(methods),
            'url': random.choice(urls),
            'status_code': random.choice(status_codes),
            'response_size': random.randint(100, 50000),
            'user_agent': f'Mozilla/5.0 (User Agent {i % 3})'
        }
        logs.append(log_entry)
    
    df = pd.DataFrame(logs)
    df = df.sort_values('timestamp')
    
    console.print(f"✅ Generated sample data: {df.shape[0]} rows × {df.shape[1]} columns")
    return df


def create_iceberg_table_minio(catalog, namespace="minio_demo"):
    """Create an Iceberg table backed by MinIO storage"""
    console.print("\n" + "="*60)
    console.print("🏗️  CREATING ICEBERG TABLE ON MINIO")
    console.print("="*60)
    
    # Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
        console.print(f"✅ Created namespace: [green]{namespace}[/green]")
    except Exception as e:
        console.print(f"ℹ️  Namespace might already exist: {e}")
    
    # Define table schema
    schema = Schema(
        NestedField(field_id=1, name="timestamp", field_type=TimestampType(), required=True),
        NestedField(field_id=2, name="ip_address", field_type=StringType(), required=True),
        NestedField(field_id=3, name="method", field_type=StringType(), required=True),
        NestedField(field_id=4, name="url", field_type=StringType(), required=True),
        NestedField(field_id=5, name="status_code", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="response_size", field_type=LongType(), required=True),
        NestedField(field_id=7, name="user_agent", field_type=StringType(), required=False),
    )
    
    # Create table
    table_name = f"{namespace}.web_logs_minio"
    
    try:
        table = catalog.create_table(
            identifier=table_name,
            schema=schema
        )
        console.print(f"✅ Created table: [green]{table_name}[/green]")
        console.print(f"   Table location: [yellow]{table.location()}[/yellow]")
        return table
        
    except Exception as e:
        console.print(f"ℹ️  Table might already exist, loading existing: {e}")
        try:
            return catalog.load_table(table_name)
        except Exception as load_error:
            console.print(f"❌ Failed to create or load table: {load_error}")
            return None


def load_data_to_minio_table(table, df):
    """Load data into MinIO-backed Iceberg table"""
    console.print("\n" + "="*60)
    console.print("📥 LOADING DATA TO MINIO-BACKED TABLE")
    console.print("="*60)
    
    try:
        # Convert to PyArrow table with proper schema
        schema = pa.schema([
            pa.field('timestamp', pa.timestamp('us'), nullable=False),
            pa.field('ip_address', pa.string(), nullable=False),
            pa.field('method', pa.string(), nullable=False),
            pa.field('url', pa.string(), nullable=False),
            pa.field('status_code', pa.int32(), nullable=False),
            pa.field('response_size', pa.int64(), nullable=False),
            pa.field('user_agent', pa.string(), nullable=True),
        ])
        
        arrow_table = pa.Table.from_pandas(df, schema=schema)
        
        console.print(f"📊 Loading {len(df)} records...")
        start_time = time.time()
        
        # Load data (this will upload to MinIO)
        table.append(arrow_table)
        
        load_time = time.time() - start_time
        console.print(f"✅ Data loaded successfully in [yellow]{load_time:.2f} seconds[/yellow]")
        console.print(f"   Average: [cyan]{len(df)/load_time:.0f} records/second[/cyan]")
        
        return True
        
    except Exception as e:
        console.print(f"❌ Failed to load data: {e}")
        return False


def inspect_minio_bucket_after_load():
    """Inspect what files were created in MinIO bucket"""
    console.print("\n" + "="*60)
    console.print("🔍 INSPECTING MINIO BUCKET STRUCTURE")
    console.print("="*60)
    
    try:
        import boto3
        
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
            use_ssl=False,
            verify=False
        )
        
        # List objects in warehouse bucket
        bucket_name = 'iceberg-warehouse'
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        objects = response.get('Contents', [])
        
        if objects:
            console.print(f"📁 Found {len(objects)} files in MinIO bucket:")
            
            # Categorize files
            data_files = []
            metadata_files = []
            manifest_files = []
            
            for obj in objects:
                key = obj['Key']
                if key.endswith('.parquet'):
                    data_files.append(obj)
                elif key.endswith('.metadata.json'):
                    metadata_files.append(obj)
                elif key.endswith('.avro'):
                    manifest_files.append(obj)
            
            # Display categorized files
            categories = [
                ("Data Files (.parquet)", data_files, "green"),
                ("Metadata Files (.metadata.json)", metadata_files, "cyan"),
                ("Manifest Files (.avro)", manifest_files, "yellow")
            ]
            
            for category_name, files, color in categories:
                if files:
                    console.print(f"\n[{color}]{category_name}[/{color}]: {len(files)} files")
                    for file_obj in files[:3]:  # Show first 3 files
                        size_mb = file_obj['Size'] / 1024 / 1024
                        console.print(f"  📄 {file_obj['Key']} ({size_mb:.2f} MB)")
                    if len(files) > 3:
                        console.print(f"  ... and {len(files) - 3} more files")
                        
        else:
            console.print("📁 No files found in bucket (this might indicate an issue)")
            
    except Exception as e:
        console.print(f"❌ Could not inspect bucket: {e}")


def query_minio_data(table):
    """Query data from MinIO-backed table"""
    console.print("\n" + "="*60)
    console.print("🔍 QUERYING MINIO-BACKED DATA")
    console.print("="*60)
    
    try:
        # Basic queries to test performance
        queries = [
            ("Count all records", lambda: len(table.scan().to_arrow())),
            ("Count by status code", lambda: table.scan().to_arrow().to_pandas()['status_code'].value_counts()),
            ("Filter 200 status", lambda: len(table.scan(row_filter="status_code == 200").to_arrow())),
            ("Select specific columns", lambda: len(table.scan(selected_fields=["timestamp", "ip_address"]).to_arrow())),
        ]
        
        results = {}
        
        for query_name, query_func in queries:
            console.print(f"\n🔍 Running: [bold]{query_name}[/bold]")
            start_time = time.time()
            
            try:
                result = query_func()
                query_time = time.time() - start_time
                
                console.print(f"   ⏱️  Completed in [yellow]{query_time:.3f} seconds[/yellow]")
                
                if isinstance(result, int):
                    console.print(f"   📊 Result: [green]{result:,} records[/green]")
                elif hasattr(result, 'head'):
                    console.print(f"   📊 Top results:")
                    for idx, value in result.head().items():
                        console.print(f"      {idx}: {value}")
                
                results[query_name] = {'time': query_time, 'result': result}
                
            except Exception as query_error:
                console.print(f"   ❌ Query failed: {query_error}")
                results[query_name] = {'time': None, 'error': str(query_error)}
        
        return results
        
    except Exception as e:
        console.print(f"❌ Querying failed: {e}")
        return {}


def compare_with_local_performance():
    """Compare MinIO performance characteristics with local filesystem"""
    console.print("\n" + "="*60)
    console.print("⚡ PERFORMANCE COMPARISON")
    console.print("="*60)
    
    console.print("📈 MinIO Object Storage characteristics:")
    console.print("   ✅ Advantages:")
    console.print("      • Scalable to petabytes")
    console.print("      • Built-in durability and replication")
    console.print("      • Multi-client access")
    console.print("      • Cost-effective for large datasets")
    
    console.print("   ⚠️  Considerations:")
    console.print("      • Network latency for each operation")
    console.print("      • Metadata operations require network calls")
    console.print("      • Smaller files have higher overhead")
    console.print("      • Bandwidth limitations")
    
    console.print("\n💡 Optimization strategies:")
    console.print("   • Batch operations when possible")
    console.print("   • Use appropriate file sizes (>= 128MB)")
    console.print("   • Co-locate compute and storage when possible")
    console.print("   • Cache frequently accessed metadata")


def main():
    """Main execution flow"""
    console.print("🏗️  Iceberg Basic Operations with MinIO")
    console.print("=" * 60)
    
    # Check prerequisites
    try:
        catalog = load_catalog('minio_local')
        console.print("✅ Catalog loaded successfully")
    except Exception as e:
        console.print(f"❌ Could not load catalog: {e}")
        console.print("   Make sure you've run 02_catalog_setup.py first")
        return False
    
    # Execution sequence
    operations_completed = 0
    total_operations = 5
    
    # 1. Generate sample data
    console.print("\n1️⃣  Generating sample data")
    df = generate_sample_data(1000)
    operations_completed += 1
    
    # 2. Create Iceberg table
    console.print("\n2️⃣  Creating Iceberg table")
    table = create_iceberg_table_minio(catalog)
    if not table:
        return False
    operations_completed += 1
    
    # 3. Load data
    console.print("\n3️⃣  Loading data to MinIO")
    if load_data_to_minio_table(table, df):
        operations_completed += 1
    
    # 4. Inspect bucket
    console.print("\n4️⃣  Inspecting MinIO bucket structure")
    inspect_minio_bucket_after_load()
    operations_completed += 1
    
    # 5. Query data
    console.print("\n5️⃣  Querying MinIO-backed data")
    results = query_minio_data(table)
    if results:
        operations_completed += 1
    
    # Performance comparison (informational)
    compare_with_local_performance()
    
    # Results summary
    console.print("\n" + "="*60)
    console.print("📊 OPERATIONS SUMMARY")
    console.print("="*60)
    
    if operations_completed == total_operations:
        console.print(f"🎉 All operations completed! ({operations_completed}/{total_operations})")
        console.print("\n✅ Successfully demonstrated Iceberg + MinIO integration!")
        console.print(f"   📊 Table location: [yellow]{table.location()}[/yellow]")
        console.print("   Next step: Run [bold]04_production_patterns.py[/bold]")
        return True
    else:
        console.print(f"⚠️  Some operations failed ({operations_completed}/{total_operations})")
        console.print("\n🔧 Check the errors above and ensure MinIO is accessible")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)