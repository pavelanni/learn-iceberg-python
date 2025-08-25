#!/usr/bin/env python3
"""
Step 7: Backup and Disaster Recovery Demo

This script demonstrates:
- Cross-region replication strategies  
- Point-in-time recovery using Iceberg snapshots
- Backup validation and integrity checking
- Disaster recovery runbooks and procedures
- Business continuity planning
- Recovery time/point objectives (RTO/RPO)
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.tree import Tree

console = Console()

def load_environment():
    """Load environment configuration"""
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file, override=True)
        console.print(f"✅ Loaded environment from: [green]{env_file}[/green]")
        return True
    else:
        console.print(f"❌ Environment file not found: {env_file}")
        return False

def get_s3_client():
    """Get S3 client for MinIO operations"""
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    region = os.getenv('MINIO_REGION', 'us-east-1')
    
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        use_ssl=endpoint.startswith('https'),
        verify=True if endpoint.startswith('https') else False
    )

def get_iceberg_catalog():
    """Get Iceberg catalog for metadata operations"""
    catalog_config = {
        "type": "sql",
        "uri": os.getenv('PYICEBERG_CATALOG__MINIO_LOCAL__URI', 'sqlite:///catalog.db'),
        "warehouse": os.getenv('PYICEBERG_CATALOG__MINIO_LOCAL__WAREHOUSE', 's3://iceberg-warehouse/'),
        "s3.endpoint": os.getenv('MINIO_ENDPOINT'),
        "s3.access-key-id": os.getenv('MINIO_ACCESS_KEY'),
        "s3.secret-access-key": os.getenv('MINIO_SECRET_KEY'),
        "s3.region": os.getenv('MINIO_REGION', 'us-east-1'),
        "s3.path-style-access": "true"
    }
    
    return SqlCatalog("minio_local", **catalog_config)

def display_dr_overview():
    """Display disaster recovery concepts overview"""
    dr_panel = Panel.fit(
        """[bold cyan]Disaster Recovery Architecture[/bold cyan]

🎯 [bold]Key Objectives:[/bold]
• [green]RTO (Recovery Time Objective)[/green] - How fast can we recover?
• [yellow]RPO (Recovery Point Objective)[/yellow] - How much data can we lose?
• [blue]Business Continuity[/blue] - Keep critical operations running
• [magenta]Data Integrity[/magenta] - Ensure recovered data is consistent

🏗️  [bold]Recovery Strategies:[/bold]
• [green]Cold Standby[/green] - Backup data, manual recovery process
• [yellow]Warm Standby[/yellow] - Replicated data, quick switchover
• [blue]Hot Standby[/blue] - Active-passive with automatic failover
• [magenta]Active-Active[/magenta] - Multiple active sites with load balancing

📊 [bold]Iceberg Advantages for DR:[/bold]
• Time travel - Point-in-time recovery to any snapshot
• ACID transactions - Consistent state even during failures
• Metadata separation - Fast recovery with minimal data movement
• Schema evolution - Forward/backward compatibility during recovery""",
        title="Disaster Recovery Overview",
        border_style="red"
    )
    console.print(dr_panel)

def create_sample_data_for_backup():
    """Create sample data with multiple snapshots for backup demonstration"""
    console.print("\n" + "="*60)
    console.print("📊 CREATING SAMPLE DATA FOR BACKUP DEMO")
    console.print("="*60)
    
    try:
        catalog = get_iceberg_catalog()
        
        # Create a sample table with transaction history
        table_name = "backup_demo.transactions"
        
        # Check if table exists and drop it
        try:
            if catalog.table_exists(table_name):
                catalog.drop_table(table_name)
                console.print(f"🗑️  Dropped existing table: {table_name}")
        except Exception:
            pass  # Table doesn't exist
        
        # Create table schema
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, DoubleType, TimestampType, LongType
        
        schema = Schema(
            NestedField(1, "transaction_id", LongType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "amount", DoubleType(), required=True),
            NestedField(4, "transaction_type", StringType(), required=True),
            NestedField(5, "timestamp", TimestampType(), required=True),
        )
        
        # Create table
        table = catalog.create_table(table_name, schema)
        console.print(f"✅ Created table: [green]{table_name}[/green]")
        
        # Add multiple snapshots with different data
        snapshots_info = []
        
        for i in range(3):
            # Generate sample data for each snapshot
            base_time = datetime.now() - timedelta(hours=i*2)
            
            data = []
            for j in range(100):
                data.append({
                    "transaction_id": i * 100 + j + 1,
                    "user_id": f"user_{j % 20 + 1:03d}",
                    "amount": round((j + 1) * (i + 1) * 10.50, 2),
                    "transaction_type": ["purchase", "refund", "transfer"][j % 3],
                    "timestamp": base_time + timedelta(minutes=j)
                })
            
            df = pd.DataFrame(data)
            
            # Write to table
            table.append(df)
            
            # Get snapshot info
            current_snapshot = table.current_snapshot()
            snapshots_info.append({
                "snapshot_id": current_snapshot.snapshot_id,
                "timestamp": current_snapshot.timestamp_ms / 1000,
                "records": len(data),
                "description": f"Batch {i+1}: {len(data)} transactions"
            })
            
            console.print(f"📝 Added snapshot {i+1}: [cyan]{current_snapshot.snapshot_id}[/cyan] ({len(data)} records)")
            time.sleep(1)  # Small delay to ensure different timestamps
        
        # Display snapshot history
        snapshot_table = Table()
        snapshot_table.add_column("Snapshot", style="cyan")
        snapshot_table.add_column("Timestamp", style="green")
        snapshot_table.add_column("Records", style="yellow")
        snapshot_table.add_column("Description", style="white")
        
        for snap in snapshots_info:
            snapshot_table.add_row(
                str(snap["snapshot_id"])[:12] + "...",
                datetime.fromtimestamp(snap["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
                str(snap["records"]),
                snap["description"]
            )
        
        console.print("\n📊 Snapshot history:")
        console.print(snapshot_table)
        
        return table, snapshots_info
        
    except Exception as e:
        console.print(f"❌ Failed to create sample data: {e}")
        return None, []

def demonstrate_backup_strategies(s3_client, table, snapshots):
    """Demonstrate different backup strategies"""
    console.print("\n" + "="*60)
    console.print("💾 BACKUP STRATEGIES DEMONSTRATION")
    console.print("="*60)
    
    backup_strategies = [
        {
            "name": "Full Backup",
            "description": "Complete copy of all data and metadata",
            "frequency": "Weekly",
            "storage_cost": "High",
            "recovery_time": "Fast", 
            "use_case": "Complete disaster recovery"
        },
        {
            "name": "Incremental Backup", 
            "description": "Only changes since last backup",
            "frequency": "Daily",
            "storage_cost": "Low",
            "recovery_time": "Medium",
            "use_case": "Regular point-in-time recovery"
        },
        {
            "name": "Metadata Backup",
            "description": "Catalog and schema information only", 
            "frequency": "Hourly",
            "storage_cost": "Very Low",
            "recovery_time": "Very Fast",
            "use_case": "Schema recovery and table structure"
        },
        {
            "name": "Snapshot Backup",
            "description": "Iceberg snapshot references",
            "frequency": "Real-time",
            "storage_cost": "Minimal", 
            "recovery_time": "Instant",
            "use_case": "Time travel and version control"
        }
    ]
    
    backup_table = Table()
    backup_table.add_column("Strategy", style="cyan")
    backup_table.add_column("Description", style="white", width=25)
    backup_table.add_column("Frequency", style="green")
    backup_table.add_column("Storage Cost", style="yellow")
    backup_table.add_column("Recovery Time", style="blue")
    backup_table.add_column("Best For", style="magenta", width=20)
    
    for strategy in backup_strategies:
        backup_table.add_row(
            strategy["name"],
            strategy["description"],
            strategy["frequency"], 
            strategy["storage_cost"],
            strategy["recovery_time"],
            strategy["use_case"]
        )
    
    console.print(backup_table)
    
    # Simulate backup creation
    console.print("\n🔧 Simulating backup creation...")
    
    bucket_name = "iceberg-warehouse"
    backup_bucket = "iceberg-backup"
    
    # Try to create backup bucket
    try:
        s3_client.create_bucket(Bucket=backup_bucket)
        console.print(f"✅ Created backup bucket: [green]{backup_bucket}[/green]")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyExists':
            console.print(f"ℹ️  Backup bucket already exists: [yellow]{backup_bucket}[/yellow]")
        else:
            console.print(f"⚠️  Could not create backup bucket: {e}")
    
    # Simulate metadata backup
    console.print("📋 Creating metadata backup...")
    metadata_backup = {
        "backup_timestamp": datetime.now().isoformat(),
        "table_name": "backup_demo.transactions",
        "snapshots": snapshots,
        "schema_version": 1,
        "warehouse_location": "s3://iceberg-warehouse/",
        "backup_strategy": "metadata"
    }
    
    backup_key = f"backups/metadata/{datetime.now().strftime('%Y/%m/%d')}/backup-{int(time.time())}.json"
    
    try:
        s3_client.put_object(
            Bucket=backup_bucket,
            Key=backup_key,
            Body=json.dumps(metadata_backup, indent=2),
            ContentType="application/json"
        )
        console.print(f"✅ Metadata backup created: [cyan]s3://{backup_bucket}/{backup_key}[/cyan]")
    except Exception as e:
        console.print(f"⚠️  Could not create metadata backup: {e}")

def demonstrate_cross_region_replication(s3_client):
    """Demonstrate cross-region replication setup"""
    console.print("\n" + "="*60)  
    console.print("🌍 CROSS-REGION REPLICATION DEMO")
    console.print("="*60)
    
    replication_panel = Panel.fit(
        """[bold cyan]Cross-Region Replication Strategy[/bold cyan]

🎯 [bold]Objectives:[/bold]
• [green]Geographic Redundancy[/green] - Protect against regional disasters
• [yellow]Compliance[/yellow] - Meet data residency requirements  
• [blue]Performance[/blue] - Serve data from nearest region
• [magenta]Availability[/magenta] - Continue operations during outages

🏗️  [bold]Implementation Options:[/bold]
• [green]Native S3 Replication[/green] - Built-in cross-region copying
• [yellow]Application-Level[/yellow] - Custom replication logic
• [blue]CDC Pipeline[/blue] - Change data capture with streaming
• [magenta]Backup-Based[/magenta] - Scheduled backup to remote region

⚡ [bold]Iceberg Benefits for Replication:[/bold]
• Atomic snapshots ensure consistent replication points
• Metadata versioning tracks replication status
• Schema evolution maintains compatibility across regions
• Time travel enables point-in-time consistency checks""",
        title="Cross-Region Replication",
        border_style="blue"
    )
    console.print(replication_panel)
    
    # Simulate replication configuration
    replication_configs = [
        {
            "source_region": "us-east-1",
            "destination_region": "us-west-2", 
            "replication_type": "Asynchronous",
            "lag_target": "< 15 minutes",
            "use_case": "Disaster Recovery"
        },
        {
            "source_region": "us-east-1",
            "destination_region": "eu-west-1",
            "replication_type": "Scheduled",
            "lag_target": "< 4 hours", 
            "use_case": "Compliance (GDPR)"
        },
        {
            "source_region": "us-east-1", 
            "destination_region": "ap-southeast-1",
            "replication_type": "On-Demand",
            "lag_target": "As needed",
            "use_case": "Analytics Workload"
        }
    ]
    
    repl_table = Table()
    repl_table.add_column("Source Region", style="green")
    repl_table.add_column("Destination", style="blue") 
    repl_table.add_column("Type", style="cyan")
    repl_table.add_column("Lag Target", style="yellow")
    repl_table.add_column("Use Case", style="magenta")
    
    for config in replication_configs:
        repl_table.add_row(
            config["source_region"],
            config["destination_region"],
            config["replication_type"],
            config["lag_target"], 
            config["use_case"]
        )
    
    console.print(repl_table)

def demonstrate_point_in_time_recovery(table, snapshots):
    """Demonstrate point-in-time recovery using Iceberg snapshots"""
    console.print("\n" + "="*60)
    console.print("⏰ POINT-IN-TIME RECOVERY DEMO")
    console.print("="*60)
    
    if not snapshots:
        console.print("❌ No snapshots available for recovery demo")
        return
    
    console.print("🎯 Scenario: Data corruption detected, need to recover to previous state")
    
    # Show current state
    current_count = len(table.scan().to_pandas())
    console.print(f"📊 Current table state: [yellow]{current_count} records[/yellow]")
    
    # Simulate corruption (we'll just conceptually show this)
    console.print("💥 [red]SIMULATED CORRUPTION DETECTED[/red]")
    console.print("   • Invalid data inserted at 2024-01-15 14:30:00")
    console.print("   • Need to recover to snapshot before corruption")
    
    # Show available recovery points
    recovery_table = Table()
    recovery_table.add_column("Snapshot ID", style="cyan")
    recovery_table.add_column("Timestamp", style="green")
    recovery_table.add_column("Records", style="yellow")
    recovery_table.add_column("Status", style="white")
    
    for i, snap in enumerate(snapshots):
        status = "✅ Clean" if i < len(snapshots) - 1 else "💥 Corrupted"
        recovery_table.add_row(
            str(snap["snapshot_id"])[:12] + "...",
            datetime.fromtimestamp(snap["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
            str(snap["records"]),
            status
        )
    
    console.print("\n🔍 Available recovery points:")
    console.print(recovery_table)
    
    # Select recovery point (second to last snapshot)
    if len(snapshots) >= 2:
        recovery_snapshot = snapshots[-2]  # Second to last
        
        console.print(f"\n🎯 Recovering to snapshot: [cyan]{recovery_snapshot['snapshot_id']}[/cyan]")
        console.print(f"   Timestamp: {datetime.fromtimestamp(recovery_snapshot['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # In a real scenario, you would:
        # 1. Create a new table from the clean snapshot
        # 2. Update applications to use the recovered table
        # 3. Verify data integrity
        # 4. Update monitoring and alerting
        
        recovery_steps = [
            "🔍 Validate selected recovery point",
            "📋 Create recovery table from snapshot", 
            "🔄 Update application connections",
            "✅ Verify data integrity post-recovery",
            "📊 Update monitoring dashboards",
            "📝 Document incident and lessons learned"
        ]
        
        console.print("\n🚀 Recovery process:")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            for step in recovery_steps:
                task = progress.add_task(step, total=1)
                time.sleep(1)  # Simulate processing time
                progress.update(task, completed=1)
        
        console.print("✅ [green]Recovery completed successfully![/green]")
        console.print(f"   Recovered {recovery_snapshot['records']} records")
        console.print(f"   Recovery Time: ~6 minutes (RTO)")
        console.print(f"   Data Loss: 0 records (RPO)")

def demonstrate_backup_validation():
    """Demonstrate backup validation and integrity checking"""
    console.print("\n" + "="*60)
    console.print("🔍 BACKUP VALIDATION DEMO") 
    console.print("="*60)
    
    validation_panel = Panel.fit(
        """[bold cyan]Backup Validation Strategy[/bold cyan]

🎯 [bold]Why Validate Backups?[/bold]
• Ensure backups are actually usable when needed
• Detect corruption or incomplete backups early
• Verify recovery procedures work as expected
• Meet compliance and audit requirements

🔧 [bold]Validation Methods:[/bold]
• [green]Checksum Verification[/green] - File integrity checks
• [yellow]Schema Validation[/yellow] - Structure consistency
• [blue]Data Sampling[/blue] - Content spot checks
• [magenta]Full Restore Test[/magenta] - Complete recovery simulation

⚡ [bold]Automated Validation Pipeline:[/bold]
• Scheduled integrity checks (daily/weekly)
• Alert on validation failures
• Track validation metrics and trends
• Document validation procedures""",
        title="Backup Validation",
        border_style="green"
    )
    console.print(validation_panel)
    
    # Simulate validation checks
    validation_checks = [
        {
            "check": "Metadata Integrity",
            "description": "Verify catalog metadata is complete",
            "status": "PASS",
            "details": "All table schemas present and valid"
        },
        {
            "check": "File Checksums", 
            "description": "Verify data file integrity",
            "status": "PASS",
            "details": "All parquet files have valid checksums"
        },
        {
            "check": "Snapshot Consistency",
            "description": "Verify snapshot references are valid", 
            "status": "PASS",
            "details": "All snapshots reference existing data files"
        },
        {
            "check": "Recovery Test",
            "description": "Test actual recovery procedure",
            "status": "PASS", 
            "details": "Recovered 300 records in 5.2 minutes"
        },
        {
            "check": "Cross-Region Sync",
            "description": "Verify backup replication status",
            "status": "WARNING",
            "details": "Backup lag: 45 minutes (target: < 30 min)"
        }
    ]
    
    validation_table = Table()
    validation_table.add_column("Validation Check", style="cyan")
    validation_table.add_column("Description", style="white", width=25)
    validation_table.add_column("Status", style="bold") 
    validation_table.add_column("Details", style="dim", width=25)
    
    for check in validation_checks:
        if check["status"] == "PASS":
            status_style = "[green]✅ PASS[/green]"
        elif check["status"] == "WARNING":
            status_style = "[yellow]⚠️  WARNING[/yellow]"
        else:
            status_style = "[red]❌ FAIL[/red]"
            
        validation_table.add_row(
            check["check"],
            check["description"],
            status_style,
            check["details"]
        )
    
    console.print(validation_table)

def create_disaster_recovery_runbook():
    """Create a disaster recovery runbook"""
    console.print("\n" + "="*60)
    console.print("📖 DISASTER RECOVERY RUNBOOK")
    console.print("="*60)
    
    # Create runbook structure
    runbook_tree = Tree("📖 [bold cyan]Disaster Recovery Runbook[/bold cyan]")
    
    # Detection and Assessment
    detection_branch = runbook_tree.add("🚨 [bold]1. Detection and Assessment[/bold]")
    detection_branch.add("Monitor alerts and health checks")
    detection_branch.add("Assess scope and impact of incident") 
    detection_branch.add("Determine if DR activation is needed")
    detection_branch.add("Notify incident response team")
    
    # Immediate Response
    response_branch = runbook_tree.add("⚡ [bold]2. Immediate Response[/bold]")
    response_branch.add("Activate incident command center")
    response_branch.add("Isolate affected systems if needed")
    response_branch.add("Switch to backup/secondary systems")
    response_branch.add("Communicate with stakeholders")
    
    # Recovery Process
    recovery_branch = runbook_tree.add("🔧 [bold]3. Recovery Process[/bold]")
    recovery_branch.add("Identify last known good state")
    recovery_branch.add("Select appropriate recovery point")
    recovery_branch.add("Execute recovery procedures")
    recovery_branch.add("Validate recovered data integrity")
    
    # Validation and Testing
    validation_branch = runbook_tree.add("✅ [bold]4. Validation and Testing[/bold]")
    validation_branch.add("Test critical application functionality")
    validation_branch.add("Verify data consistency and completeness") 
    validation_branch.add("Performance testing and optimization")
    validation_branch.add("User acceptance testing")
    
    # Post-Recovery
    post_branch = runbook_tree.add("📊 [bold]5. Post-Recovery Activities[/bold]")
    post_branch.add("Monitor system stability")
    post_branch.add("Document lessons learned")
    post_branch.add("Update procedures and runbooks")
    post_branch.add("Conduct post-incident review")
    
    console.print(runbook_tree)
    
    # Emergency contacts and key information
    console.print("\n📞 Emergency contacts:")
    contacts_table = Table()
    contacts_table.add_column("Role", style="cyan")
    contacts_table.add_column("Contact", style="green")
    contacts_table.add_column("Backup", style="yellow")
    
    contacts_table.add_row("Incident Commander", "alice@company.com", "bob@company.com")
    contacts_table.add_row("Technical Lead", "charlie@company.com", "diana@company.com")
    contacts_table.add_row("Business Stakeholder", "eve@company.com", "frank@company.com")
    contacts_table.add_row("External Vendor", "support@minio.com", "1-800-MINIO-HELP")
    
    console.print(contacts_table)
    
    # Key system information
    console.print("\n🔧 Key system information:")
    system_info = [
        "Primary MinIO Cluster: https://minio-prod.company.com",
        "Backup MinIO Cluster: https://minio-dr.company.com", 
        "Iceberg Catalog: https://iceberg-catalog.company.com",
        "Monitoring Dashboard: https://grafana.company.com/dr",
        "Recovery Scripts: /opt/scripts/disaster-recovery/",
        "Documentation: https://wiki.company.com/disaster-recovery"
    ]
    
    for info in system_info:
        console.print(f"  • {info}")

def main():
    """Main execution flow"""
    console.print("🆘 MinIO Backup and Disaster Recovery Demo")
    console.print("=" * 60)
    
    # Load environment
    if not load_environment():
        console.print("❌ Cannot proceed without environment configuration")
        return False
    
    # Display DR overview
    display_dr_overview()
    
    # Get clients
    try:
        s3_client = get_s3_client()
    except Exception as e:
        console.print(f"❌ Failed to create S3 client: {e}")
        return False
    
    # Create sample data
    table, snapshots = create_sample_data_for_backup()
    
    # Demonstrate backup strategies
    demonstrate_backup_strategies(s3_client, table, snapshots)
    
    # Show cross-region replication
    demonstrate_cross_region_replication(s3_client)
    
    # Demonstrate point-in-time recovery
    if table and snapshots:
        demonstrate_point_in_time_recovery(table, snapshots)
    
    # Show backup validation
    demonstrate_backup_validation()
    
    # Create DR runbook
    create_disaster_recovery_runbook()
    
    # Summary
    console.print("\n" + "="*60)
    console.print("🎯 BACKUP AND DISASTER RECOVERY DEMO COMPLETE") 
    console.print("="*60)
    
    console.print("✅ [green]What we covered:[/green]")
    console.print("• Backup strategies and implementation patterns")
    console.print("• Cross-region replication for geographic redundancy")
    console.print("• Point-in-time recovery using Iceberg snapshots")
    console.print("• Backup validation and integrity checking")
    console.print("• Disaster recovery procedures and runbooks")
    
    console.print("\n📝 [yellow]Next steps for production:[/yellow]")
    console.print("• Set up automated backup schedules")
    console.print("• Implement cross-region replication")
    console.print("• Create and test disaster recovery procedures")
    console.print("• Set up monitoring and alerting for backup systems")
    console.print("• Train team on emergency response procedures")
    console.print("• Regular DR drills and procedure updates")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)