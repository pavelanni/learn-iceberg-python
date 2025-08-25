#!/usr/bin/env python3
"""
Step 5: Bucket Versioning and Lifecycle Management

This script demonstrates:
- MinIO bucket versioning for data protection
- Object lifecycle management policies
- Handling corrupted data with versioning
- Cost optimization through intelligent tiering
- Backup and recovery strategies for Iceberg tables
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def load_environment():
    """Load environment configuration"""
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
        return True
    return False


def get_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        region_name=os.getenv('MINIO_REGION', 'us-east-1'),
        use_ssl=False,
        verify=False
    )


def demonstrate_bucket_versioning():
    """Demonstrate bucket versioning for data protection"""
    console.print("\n" + "="*60)
    console.print("📦 BUCKET VERSIONING DEMONSTRATION")
    console.print("="*60)
    
    s3_client = get_minio_client()
    versioned_bucket = 'iceberg-versioned-demo'
    
    try:
        # Create versioned bucket
        console.print(f"🔧 Creating versioned bucket: [bold]{versioned_bucket}[/bold]")
        
        try:
            s3_client.create_bucket(Bucket=versioned_bucket)
            console.print("✅ Bucket created successfully")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                console.print("ℹ️  Bucket already exists, continuing...")
            else:
                raise
        
        # Enable versioning
        console.print("🔄 Enabling bucket versioning...")
        s3_client.put_bucket_versioning(
            Bucket=versioned_bucket,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Verify versioning status
        response = s3_client.get_bucket_versioning(Bucket=versioned_bucket)
        status = response.get('Status', 'Disabled')
        console.print(f"✅ Versioning status: [green]{status}[/green]")
        
        # Demonstrate versioning with multiple uploads
        console.print("\n📝 Creating multiple versions of the same object...")
        
        test_key = 'demo/versioned-file.txt'
        versions = []
        
        for version_num in range(1, 4):
            content = f"This is version {version_num} of the file. Timestamp: {datetime.now()}"
            
            console.print(f"   Uploading version {version_num}...")
            response = s3_client.put_object(
                Bucket=versioned_bucket,
                Key=test_key,
                Body=content,
                ContentType='text/plain'
            )
            
            version_id = response.get('VersionId')
            versions.append((version_num, version_id, content[:30] + "..."))
            console.print(f"   └── Version ID: [cyan]{version_id}[/cyan]")
            
            time.sleep(1)  # Small delay to ensure different timestamps
        
        # List all versions
        console.print(f"\n📋 Listing all versions of [yellow]{test_key}[/yellow]:")
        
        response = s3_client.list_object_versions(
            Bucket=versioned_bucket,
            Prefix=test_key
        )
        
        version_table = Table(title="Object Versions")
        version_table.add_column("Version", style="cyan")
        version_table.add_column("Version ID", style="yellow")
        version_table.add_column("Last Modified", style="green")
        version_table.add_column("Size", style="magenta")
        version_table.add_column("Is Latest", style="red")
        
        versions_found = response.get('Versions', [])
        for i, version in enumerate(sorted(versions_found, key=lambda x: x['LastModified'], reverse=True)):
            version_table.add_row(
                f"v{len(versions_found) - i}",
                version['VersionId'][:8] + "...",
                version['LastModified'].strftime('%H:%M:%S'),
                f"{version['Size']} bytes",
                "✅ Latest" if version.get('IsLatest', False) else "📄 Previous"
            )
        
        console.print(version_table)
        
        # Demonstrate recovery from "corruption"
        console.print(f"\n💥 Simulating data corruption...")
        corrupt_content = "CORRUPTED DATA - This file has been damaged!"
        s3_client.put_object(
            Bucket=versioned_bucket,
            Key=test_key,
            Body=corrupt_content
        )
        console.print("❌ File 'corrupted' with bad data")
        
        # Show current content is corrupted
        response = s3_client.get_object(Bucket=versioned_bucket, Key=test_key)
        current_content = response['Body'].read().decode('utf-8')
        console.print(f"   Current content: [red]{current_content}[/red]")
        
        # Recover from previous version
        console.print(f"\n🔄 Recovering from previous version...")
        if len(versions) >= 2:
            good_version_id = versions[-2][1]  # Second-to-last version
            
            # Get the good version
            response = s3_client.get_object(
                Bucket=versioned_bucket,
                Key=test_key,
                VersionId=good_version_id
            )
            good_content = response['Body'].read().decode('utf-8')
            
            # Restore by uploading as new version
            s3_client.put_object(
                Bucket=versioned_bucket,
                Key=test_key,
                Body=good_content
            )
            
            console.print("✅ File recovered from previous version")
            console.print(f"   Restored content: [green]{good_content[:50]}...[/green]")
        
        return versioned_bucket
        
    except Exception as e:
        console.print(f"❌ Versioning demo failed: {e}")
        return None


def demonstrate_lifecycle_policies():
    """Demonstrate lifecycle management for cost optimization"""
    console.print("\n" + "="*60)
    console.print("♻️  LIFECYCLE MANAGEMENT DEMONSTRATION")
    console.print("="*60)
    
    # Note: MinIO lifecycle policies work different than AWS S3
    console.print("📋 Lifecycle Management Concepts:")
    
    lifecycle_info = Panel.fit(
        """[bold cyan]Lifecycle Management Benefits for Iceberg:[/bold cyan]

🗂️  [bold]Data Tiering:[/bold]
   • Move old snapshots to cheaper storage
   • Archive rarely accessed data
   • Optimize storage costs automatically

⏰ [bold]Retention Policies:[/bold]
   • Automatically delete old Iceberg snapshots
   • Clean up orphaned metadata files
   • Maintain compliance with data retention rules

💰 [bold]Cost Optimization:[/bold]
   • Reduce storage costs by 50-80%
   • Automatic cleanup of temporary files
   • Smart compression and deduplication

🔄 [bold]Example Policy:[/bold]
   • Keep current data in hot storage
   • Move 30+ day data to warm storage  
   • Archive 90+ day data to cold storage
   • Delete 7+ year old data (compliance)""",
        title="Lifecycle Benefits",
        border_style="green"
    )
    console.print(lifecycle_info)
    
    # Create example lifecycle configuration
    lifecycle_config = {
        "Rules": [
            {
                "ID": "IcebergSnapshotCleanup",
                "Status": "Enabled",
                "Filter": {"Prefix": "metadata/"},
                "Transitions": [
                    {
                        "Days": 30,
                        "StorageClass": "STANDARD_IA"  # Infrequent Access
                    },
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER"      # Long-term archive
                    }
                ],
                "Expiration": {
                    "Days": 2555  # 7 years for compliance
                }
            },
            {
                "ID": "TempFileCleanup",
                "Status": "Enabled",
                "Filter": {"Prefix": "temp/"},
                "Expiration": {
                    "Days": 1  # Clean up temp files daily
                }
            },
            {
                "ID": "OldSnapshotCleanup",
                "Status": "Enabled",
                "Filter": {"Prefix": "metadata/snap-"},
                "Expiration": {
                    "Days": 365  # Keep snapshots for 1 year
                }
            }
        ]
    }
    
    console.print("\n📝 Example Lifecycle Configuration:")
    console.print(f"[dim]{json.dumps(lifecycle_config, indent=2)}[/dim]")
    
    console.print(f"\n💡 In production, you would apply this with:")
    console.print(f"[cyan]s3_client.put_bucket_lifecycle_configuration([/cyan]")
    console.print(f"[cyan]    Bucket='your-bucket',[/cyan]")
    console.print(f"[cyan]    LifecycleConfiguration=lifecycle_config[/cyan]")
    console.print(f"[cyan])[/cyan]")


def demonstrate_iceberg_versioning_integration():
    """Show how Iceberg snapshots work with MinIO versioning"""
    console.print("\n" + "="*60)
    console.print("⚡ ICEBERG + MINIO VERSIONING INTEGRATION")
    console.print("="*60)
    
    try:
        # Load Iceberg catalog
        catalog = load_catalog('minio_local')
        
        # Create a versioned table
        console.print("🔧 Creating versioned Iceberg table...")
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import (LongType, NestedField, 
                                     StringType)
        
        # Define simple schema (using LongType to match pandas int64)
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="timestamp", field_type=StringType(), required=False),
            NestedField(field_id=3, name="message", field_type=StringType(), required=False),
        )
        
        # Create namespace and table
        try:
            catalog.create_namespace("versioning_demo")
        except:
            pass  # Namespace might exist
        
        table_name = "versioning_demo.version_test_v2"
        try:
            # Try to drop existing table first
            if catalog.table_exists(table_name):
                catalog.drop_table(table_name)
                console.print(f"🗑️  Dropped existing table: {table_name}")
            
            table = catalog.create_table(table_name, schema)
            console.print(f"✅ Created table: [green]{table_name}[/green]")
        except Exception as e:
            console.print(f"⚠️  Could not create/recreate table: {e}")
            return None
        
        # Add multiple snapshots to show versioning
        console.print("\n📊 Creating multiple Iceberg snapshots...")
        
        for snapshot_num in range(1, 4):
            # Generate sample data
            data = {
                'id': range(snapshot_num * 100, (snapshot_num + 1) * 100),
                'timestamp': [(pd.Timestamp.now() + timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M:%S') for i in range(100)],
                'message': [f'Snapshot {snapshot_num} - Record {i}' for i in range(100)]
            }
            
            df = pd.DataFrame(data)
            arrow_table = pa.Table.from_pandas(df)
            
            console.print(f"   Adding snapshot {snapshot_num} with {len(df)} records...")
            table.append(arrow_table)
        
        # Show Iceberg snapshots
        snapshots = list(table.snapshots())
        console.print(f"\n📸 Iceberg table has {len(snapshots)} snapshots:")
        
        snapshot_table = Table(title="Iceberg Snapshots")
        snapshot_table.add_column("Snapshot ID", style="cyan")
        snapshot_table.add_column("Timestamp", style="yellow") 
        snapshot_table.add_column("Records Added", style="green")
        snapshot_table.add_column("Summary", style="magenta")
        
        for snapshot in snapshots:
            summary = snapshot.summary if snapshot.summary else {}
            snapshot_table.add_row(
                str(snapshot.snapshot_id)[:8] + "...",
                datetime.fromtimestamp(snapshot.timestamp_ms / 1000).strftime('%H:%M:%S'),
                str(summary.get('added-records', 'N/A')),
                str(summary.get('operation', 'unknown'))
            )
        
        console.print(snapshot_table)
        
        # Explain the dual versioning
        dual_versioning_info = Panel.fit(
            """[bold cyan]Dual Versioning Strategy:[/bold cyan]

📸 [bold]Iceberg Snapshots:[/bold]
   • Logical versioning of table state
   • Atomic transactions with ACID properties  
   • Time travel queries
   • Rollback capabilities

🗂️  [bold]MinIO Object Versioning:[/bold]
   • Physical versioning of individual files
   • Protection against accidental deletion
   • Recovery from file corruption
   • Compliance and audit requirements

🔄 [bold]Combined Benefits:[/bold]
   • Logical rollback: Use Iceberg snapshots
   • File recovery: Use MinIO versions
   • Complete data protection at multiple levels
   • Granular control over retention policies""",
            title="Dual Versioning Protection",
            border_style="blue"
        )
        console.print(f"\n{dual_versioning_info}")
        
    except Exception as e:
        console.print(f"❌ Iceberg versioning demo failed: {e}")


def demonstrate_backup_strategies():
    """Show backup and disaster recovery patterns"""
    console.print("\n" + "="*60)
    console.print("💾 BACKUP AND DISASTER RECOVERY")
    console.print("="*60)
    
    backup_strategies = Panel.fit(
        """[bold cyan]Iceberg + MinIO Backup Strategies:[/bold cyan]

🏠 [bold]Cross-Region Replication:[/bold]
   • Replicate buckets to multiple MinIO clusters
   • Automatic failover capabilities
   • Geographic disaster recovery

📤 [bold]Snapshot Export:[/bold]
   • Export Iceberg metadata to separate storage
   • Version control for table schemas
   • Quick recovery from metadata corruption

🔄 [bold]Continuous Sync:[/bold]
   • Real-time replication of data changes
   • Incremental backup of new snapshots only
   • Minimal RTO/RPO for critical data

📋 [bold]Metadata Backup:[/bold]
   • Regular backup of catalog database
   • Schema evolution history preservation  
   • Configuration and access policy backup

⚡ [bold]Point-in-Time Recovery:[/bold]
   • Restore to any Iceberg snapshot
   • Combined with MinIO versioning
   • Granular recovery options""",
        title="Backup Strategies",
        border_style="yellow"
    )
    console.print(backup_strategies)
    
    # Show example backup commands
    console.print("\n📝 Example Backup Commands:")
    console.print("""
[cyan]# Cross-region bucket replication[/cyan]
mc mirror source-minio/iceberg-warehouse target-minio/iceberg-warehouse-backup

[cyan]# Metadata backup[/cyan] 
sqlite3 catalog.db .dump > catalog_backup_$(date +%Y%m%d).sql

[cyan]# Selective snapshot backup[/cyan]
mc cp --recursive source-minio/bucket/metadata/ backup-location/

[cyan]# Automated backup script[/cyan]
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
mc mirror --remove --overwrite local/iceberg-warehouse remote/backup-$DATE/
    """)


def main():
    """Main execution flow"""
    console.print("⚡ Advanced MinIO Features for Iceberg")
    console.print("=" * 60)
    
    # Load environment
    if not load_environment():
        console.print("⚠️  No .env file found - using defaults")
    
    # Run demonstrations
    demos_completed = 0
    total_demos = 5
    
    # 1. Bucket versioning
    console.print("\n1️⃣  Bucket versioning demonstration")
    versioned_bucket = demonstrate_bucket_versioning()
    if versioned_bucket:
        demos_completed += 1
    
    # 2. Lifecycle policies
    console.print("\n2️⃣  Lifecycle management")
    demonstrate_lifecycle_policies()
    demos_completed += 1
    
    # 3. Iceberg integration
    console.print("\n3️⃣  Iceberg versioning integration")
    demonstrate_iceberg_versioning_integration()
    demos_completed += 1
    
    # 4. Backup strategies
    console.print("\n4️⃣  Backup and disaster recovery")
    demonstrate_backup_strategies()
    demos_completed += 1
    
    # Summary
    console.print("\n" + "="*60)
    console.print("🎯 ADVANCED FEATURES SUMMARY")
    console.print("="*60)
    
    if demos_completed >= 4:
        console.print(f"🎉 Successfully demonstrated {demos_completed} advanced features!")
        
        summary_table = Table(title="Advanced MinIO Features for Iceberg")
        summary_table.add_column("Feature", style="cyan")
        summary_table.add_column("Benefit", style="yellow")
        summary_table.add_column("Use Case", style="green")
        
        features = [
            ("Bucket Versioning", "File-level protection", "Corruption recovery"),
            ("Lifecycle Policies", "Cost optimization", "Automatic cleanup"), 
            ("Dual Versioning", "Multi-level protection", "Complete data safety"),
            ("Backup Strategies", "Disaster recovery", "Business continuity"),
        ]
        
        for feature, benefit, use_case in features:
            summary_table.add_row(feature, benefit, use_case)
        
        console.print(summary_table)
        
        console.print(f"\n💡 Next steps:")
        console.print(f"   • Implement lifecycle policies for cost savings")
        console.print(f"   • Set up cross-region replication for DR") 
        console.print(f"   • Configure automated backup schedules")
        console.print(f"   • Test recovery procedures regularly")
        
    else:
        console.print(f"⚠️  Completed {demos_completed} of {total_demos} demonstrations")
        console.print("   Some features may require additional MinIO configuration")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)