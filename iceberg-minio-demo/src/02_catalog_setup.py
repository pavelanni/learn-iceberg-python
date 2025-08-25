#!/usr/bin/env python3
"""
Step 2: Iceberg Catalog Setup with MinIO

This script demonstrates:
- Configuring PyIceberg to use MinIO as warehouse storage
- Setting up SQL catalog for development
- Testing catalog connectivity and operations
- Understanding catalog vs warehouse separation
- Debugging common configuration issues
"""

import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from rich.console import Console
from rich.table import Table

console = Console()


def load_config(config_name='local'):
    """Load configuration from YAML file"""
    config_path = Path(f"config/{config_name}.yaml")
    
    if not config_path.exists():
        console.print(f"❌ Configuration file not found: {config_path}")
        console.print("   Make sure you're running this from the iceberg-minio-demo directory")
        return None
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    console.print(f"📋 Loaded configuration from: [bold]{config_path}[/bold]")
    return config


def setup_environment_variables():
    """Load environment variables from .env file and set defaults"""
    console.print("🔐 Loading environment configuration...")
    
    # Load .env file if it exists
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
        console.print(f"✅ Loaded environment from: [green]{env_file}[/green]")
    else:
        console.print(f"ℹ️  No .env file found at {env_file}")
        console.print("   Using fallback defaults (not recommended for production)")
    
    # Set PYICEBERG_HOME if not already set
    if 'PYICEBERG_HOME' not in os.environ:
        os.environ['PYICEBERG_HOME'] = os.getcwd()
        console.print(f"🔧 Set PYICEBERG_HOME = [yellow]{os.getcwd()}[/yellow]")
    
    # Verify required environment variables are set
    required_vars = [
        'MINIO_ENDPOINT',
        'MINIO_ACCESS_KEY', 
        'MINIO_SECRET_KEY',
        'PYICEBERG_CATALOG__MINIO_LOCAL__S3__ENDPOINT',
        'PYICEBERG_CATALOG__MINIO_LOCAL__S3__ACCESS_KEY_ID',
        'PYICEBERG_CATALOG__MINIO_LOCAL__S3__SECRET_ACCESS_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        if var not in os.environ:
            missing_vars.append(var)
    
    if missing_vars:
        console.print(f"⚠️  Missing environment variables: {missing_vars}")
        console.print("💡 Create a .env file based on .env.example")
        
        # Set fallback values for development
        fallback_values = {
            'MINIO_ENDPOINT': 'http://localhost:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'MINIO_REGION': 'us-east-1',
            'PYICEBERG_CATALOG__MINIO_LOCAL__TYPE': 'sql',
            'PYICEBERG_CATALOG__MINIO_LOCAL__URI': 'sqlite:///catalog.db',
            'PYICEBERG_CATALOG__MINIO_LOCAL__WAREHOUSE': 's3://iceberg-warehouse/',
            'PYICEBERG_CATALOG__MINIO_LOCAL__S3__ENDPOINT': 'http://localhost:9000',
            'PYICEBERG_CATALOG__MINIO_LOCAL__S3__ACCESS_KEY_ID': 'minioadmin',
            'PYICEBERG_CATALOG__MINIO_LOCAL__S3__SECRET_ACCESS_KEY': 'minioadmin',
            'PYICEBERG_CATALOG__MINIO_LOCAL__S3__REGION': 'us-east-1',
            'PYICEBERG_CATALOG__MINIO_LOCAL__S3__PATH_STYLE_ACCESS': 'true'
        }
        
        for key, value in fallback_values.items():
            if key not in os.environ:
                os.environ[key] = value
                console.print(f"🔧 Fallback: {key.split('__')[-1]} = [yellow]{value}[/yellow]")
    else:
        console.print("✅ All required environment variables are set")
    
    # Display current configuration (mask sensitive values)
    console.print("\n📋 Current configuration:")
    config_display = [
        ("MinIO Endpoint", os.getenv('MINIO_ENDPOINT', 'Not set')),
        ("MinIO Region", os.getenv('MINIO_REGION', 'Not set')),
        ("Access Key", f"{os.getenv('MINIO_ACCESS_KEY', 'Not set')[:8]}..." if os.getenv('MINIO_ACCESS_KEY') else 'Not set'),
        ("Warehouse", os.getenv('PYICEBERG_CATALOG__MINIO_LOCAL__WAREHOUSE', 'Not set')),
    ]
    
    for key, value in config_display:
        console.print(f"   • [cyan]{key}[/cyan]: [yellow]{value}[/yellow]")


def create_pyiceberg_config(catalog_config):
    """Create .pyiceberg.yaml configuration file"""
    console.print("\n" + "="*60)
    console.print("📄 CREATING PYICEBERG CONFIGURATION")
    console.print("="*60)
    
    config_file = Path('.pyiceberg.yaml')
    
    # Check if config already exists
    if config_file.exists():
        console.print(f"ℹ️  Configuration file already exists: {config_file}")
        console.print("   We'll use the existing configuration")
        return str(config_file.resolve())
    
    # Create configuration
    try:
        with open(config_file, 'w') as f:
            yaml.dump(catalog_config, f, default_flow_style=False)
        
        console.print(f"✅ Created PyIceberg configuration: [green]{config_file}[/green]")
        
        # Display the configuration
        console.print("\n📋 Configuration contents:")
        with open(config_file, 'r') as f:
            content = f.read()
            console.print(f"[dim]{content}[/dim]")
        
        return str(config_file.resolve())
        
    except Exception as e:
        console.print(f"❌ Failed to create configuration file: {e}")
        return None


def test_catalog_connection(catalog_name='minio_local'):
    """Test connection to the Iceberg catalog"""
    console.print("\n" + "="*60)
    console.print("🔌 TESTING CATALOG CONNECTION")
    console.print("="*60)
    
    try:
        # Try loading with direct configuration first
        console.print(f"🔄 Loading catalog: [bold]{catalog_name}[/bold]")
        
        # Direct configuration approach as fallback
        try:
            catalog = load_catalog(
                catalog_name,
                **{
                    "type": "sql",
                    "uri": "sqlite:///catalog.db",
                    "warehouse": "s3://iceberg-warehouse/",
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "minioadmin",
                    "s3.secret-access-key": "minioadmin",
                    "s3.region": "us-east-1",
                    "s3.path-style-access": "true"
                }
            )
            console.print("✅ Loaded catalog using direct configuration")
        except Exception as direct_error:
            console.print(f"⚠️  Direct config failed: {direct_error}")
            console.print("🔄 Trying environment variable approach...")
            catalog = load_catalog(catalog_name)
        
        console.print("✅ Successfully loaded catalog!")
        console.print(f"   Catalog type: [yellow]{type(catalog).__name__}[/yellow]")
        
        # Test basic catalog operations
        console.print("\n🧪 Testing catalog operations...")
        
        # List namespaces (should be empty initially)
        try:
            namespaces = list(catalog.list_namespaces())
            console.print(f"📂 Found {len(namespaces)} namespaces: {namespaces}")
        except Exception as e:
            console.print(f"ℹ️  Namespace listing not supported or failed: {e}")
        
        # Try to list tables (should be empty initially)
        try:
            # Note: list_tables() without namespace parameter lists all tables
            tables = []
            for namespace in namespaces:
                try:
                    namespace_tables = list(catalog.list_tables(namespace))
                    tables.extend(namespace_tables)
                except Exception:
                    pass
            console.print(f"📊 Found {len(tables)} tables: {tables}")
        except Exception as e:
            console.print(f"ℹ️  Table listing failed (this is normal for empty catalog): {e}")
        
        return catalog
        
    except Exception as e:
        console.print(f"❌ Failed to connect to catalog: {e}")
        console.print("\n💡 Common issues:")
        console.print("   • MinIO not running or not accessible")
        console.print("   • Incorrect credentials in configuration")
        console.print("   • Network connectivity issues")
        console.print("   • Bucket permissions not set correctly")
        return None


def create_test_namespace(catalog):
    """Create a test namespace to verify catalog write permissions"""
    console.print("\n" + "="*60)
    console.print("📁 TESTING NAMESPACE CREATION")
    console.print("="*60)
    
    namespace_name = "minio_test"
    
    try:
        # Check if namespace already exists
        existing_namespaces = list(catalog.list_namespaces())
        if (namespace_name,) in existing_namespaces:
            console.print(f"ℹ️  Namespace [yellow]{namespace_name}[/yellow] already exists")
            return True
        
        # Create new namespace
        console.print(f"🔧 Creating namespace: [bold]{namespace_name}[/bold]")
        catalog.create_namespace(namespace_name)
        
        console.print(f"✅ Successfully created namespace: [green]{namespace_name}[/green]")
        
        # Verify creation
        updated_namespaces = list(catalog.list_namespaces())
        console.print(f"📂 Updated namespaces: {updated_namespaces}")
        
        return True
        
    except Exception as e:
        console.print(f"❌ Failed to create namespace: {e}")
        console.print("\n💡 This might indicate:")
        console.print("   • Insufficient permissions on MinIO bucket")
        console.print("   • Catalog database write issues")
        console.print("   • Configuration problems")
        return False


def display_catalog_info(catalog):
    """Display detailed catalog information"""
    console.print("\n" + "="*60)
    console.print("ℹ️  CATALOG INFORMATION")
    console.print("="*60)
    
    info_table = Table(title="Catalog Configuration")
    info_table.add_column("Property", style="cyan")
    info_table.add_column("Value", style="yellow")
    
    # Display catalog properties
    info_table.add_row("Catalog Type", type(catalog).__name__)
    
    # Try to get warehouse location (if available)
    try:
        if hasattr(catalog, '_warehouse_location'):
            info_table.add_row("Warehouse Location", str(catalog._warehouse_location))
    except:
        pass
    
    # Display current working directory
    info_table.add_row("Working Directory", os.getcwd())
    info_table.add_row("Config File", ".pyiceberg.yaml")
    
    console.print(info_table)
    
    # List current namespaces and tables
    try:
        namespaces = list(catalog.list_namespaces())
        if namespaces:
            console.print(f"\n📂 Available namespaces: {namespaces}")
            
            # List tables in each namespace
            for namespace in namespaces:
                try:
                    tables = list(catalog.list_tables(namespace))
                    if tables:
                        console.print(f"   └── Tables in {namespace}: {tables}")
                    else:
                        console.print(f"   └── No tables in {namespace}")
                except Exception as e:
                    console.print(f"   └── Could not list tables in {namespace}: {e}")
        else:
            console.print("\n📂 No namespaces found (catalog is empty)")
            
    except Exception as e:
        console.print(f"\nℹ️  Could not list namespaces: {e}")


def verify_minio_bucket_structure():
    """Check what files were created in MinIO bucket"""
    console.print("\n" + "="*60)
    console.print("🗂️  VERIFYING MINIO BUCKET STRUCTURE")
    console.print("="*60)
    
    try:
        import boto3
        
        # Get MinIO client using same config
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            region_name=os.getenv('MINIO_REGION', 'us-east-1'),
            use_ssl=False,
            verify=False
        )
        
        # List objects in warehouse bucket
        bucket_name = 'iceberg-warehouse'
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        objects = response.get('Contents', [])
        
        if objects:
            console.print(f"📁 Found {len(objects)} objects in bucket [yellow]{bucket_name}[/yellow]:")
            
            file_table = Table()
            file_table.add_column("Key", style="cyan")
            file_table.add_column("Size", style="green")
            file_table.add_column("Modified", style="yellow")
            
            for obj in objects:
                file_table.add_row(
                    obj['Key'],
                    f"{obj['Size']} bytes",
                    obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                )
            
            console.print(file_table)
        else:
            console.print(f"📁 Bucket [yellow]{bucket_name}[/yellow] is empty")
            console.print("   This is normal if no tables have been created yet")
        
    except Exception as e:
        console.print(f"ℹ️  Could not inspect MinIO bucket: {e}")


def main():
    """Main execution flow"""
    console.print("🔧 Iceberg Catalog Setup with MinIO")
    console.print("=" * 60)
    
    # Setup sequence
    steps_completed = 0
    total_steps = 5
    
    # 1. Setup environment
    console.print("\n1️⃣  Setting up environment variables")
    setup_environment_variables()
    steps_completed += 1
    
    # 2. Load configuration
    console.print("\n2️⃣  Loading configuration")
    config = load_config('local')
    if not config:
        return False
    steps_completed += 1
    
    # 3. Create PyIceberg configuration file
    console.print("\n3️⃣  Creating PyIceberg configuration")
    config_file = create_pyiceberg_config(config)
    if not config_file:
        return False
    steps_completed += 1
    
    # 4. Test catalog connection
    console.print("\n4️⃣  Testing catalog connection")
    catalog = test_catalog_connection('minio_local')
    if not catalog:
        return False
    steps_completed += 1
    
    # 5. Create test namespace
    console.print("\n5️⃣  Testing namespace operations")
    if create_test_namespace(catalog):
        steps_completed += 1
    
    # Display information (always runs)
    display_catalog_info(catalog)
    verify_minio_bucket_structure()
    
    # Results summary
    console.print("\n" + "="*60)
    console.print("📊 SETUP RESULTS SUMMARY")
    console.print("="*60)
    
    if steps_completed == total_steps:
        console.print(f"🎉 Catalog setup complete! ({steps_completed}/{total_steps})")
        console.print("\n✅ Your Iceberg catalog is ready to use with MinIO!")
        console.print("   Next step: Run [bold]03_basic_operations.py[/bold]")
        return True
    else:
        console.print(f"⚠️  Setup partially complete ({steps_completed}/{total_steps})")
        console.print("\n🔧 Please fix any issues above before proceeding")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)