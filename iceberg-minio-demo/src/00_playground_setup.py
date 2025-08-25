#!/usr/bin/env python3
"""
Step 0: MinIO Playground Setup (Optional)

This script demonstrates:
- Connecting to the MinIO public playground server
- Understanding playground limitations and use cases
- Setting up for remote testing without local infrastructure
- Comparing playground vs local development workflows
"""

import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def load_playground_environment():
    """Load MinIO playground environment configuration"""
    env_file = Path('config/playground.env')
    if env_file.exists():
        load_dotenv(env_file, override=True)
        console.print(f"✅ Loaded playground environment from: [green]{env_file}[/green]")
        return True
    else:
        console.print(f"❌ Playground environment file not found: {env_file}")
        return False


def display_playground_info():
    """Display information about the MinIO playground"""
    info_panel = Panel.fit(
        """[bold cyan]MinIO Playground Server[/bold cyan]

🌐 [bold]Endpoint:[/bold] https://play.min.io
👤 [bold]Access Key:[/bold] Q3AM3UQ867SPQQA43P2F
🔑 [bold]Secret Key:[/bold] zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG

⚠️  [yellow]Important Limitations:[/yellow]
• Data is [red]temporary[/red] and may be deleted at any time
• Buckets are [red]shared[/red] with other users
• Rate limiting may apply
• [red]No persistence[/red] between sessions
• Use only for [cyan]learning and testing[/cyan]

✅ [green]Good for:[/green]
• Learning Iceberg concepts
• Testing configurations
• Demos and tutorials
• CI/CD testing

❌ [red]Not suitable for:[/red]
• Production workloads
• Sensitive data
• Long-term storage
• Performance testing""",
        title="MinIO Playground",
        border_style="cyan"
    )
    console.print(info_panel)


def test_playground_connection():
    """Test connection to MinIO playground"""
    console.print("\n" + "="*60)
    console.print("🧪 TESTING PLAYGROUND CONNECTION")
    console.print("="*60)
    
    try:
        # Get playground credentials
        endpoint = os.getenv('MINIO_ENDPOINT')
        access_key = os.getenv('MINIO_ACCESS_KEY')
        secret_key = os.getenv('MINIO_SECRET_KEY')
        region = os.getenv('MINIO_REGION', 'us-east-1')
        
        console.print(f"🔗 Connecting to: [bold blue]{endpoint}[/bold blue]")
        
        # Create S3 client for playground
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=True,  # Playground uses HTTPS
            verify=True
        )
        
        # Test basic connectivity
        response = s3_client.list_buckets()
        
        console.print("✅ Successfully connected to MinIO playground!")
        
        # Display existing buckets (may be from other users)
        buckets = response.get('Buckets', [])
        if buckets:
            console.print(f"\n📂 Found {len(buckets)} existing buckets (may include other users' buckets):")
            
            bucket_table = Table()
            bucket_table.add_column("Bucket Name", style="cyan")
            bucket_table.add_column("Created", style="green")
            bucket_table.add_column("Note", style="yellow")
            
            for bucket in buckets[:10]:  # Show only first 10
                note = "Shared bucket" if not bucket['Name'].startswith('iceberg-') else "Your bucket?"
                bucket_table.add_row(
                    bucket['Name'],
                    bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S'),
                    note
                )
            
            console.print(bucket_table)
            
            if len(buckets) > 10:
                console.print(f"... and {len(buckets) - 10} more buckets")
        else:
            console.print("📂 No existing buckets found")
        
        return s3_client
        
    except NoCredentialsError:
        console.print("❌ Authentication failed - check playground credentials")
        return None
    except ClientError as e:
        error_code = e.response['Error']['Code']
        console.print(f"❌ AWS Client Error: [bold red]{error_code}[/bold red]")
        console.print(f"   Message: {e.response['Error']['Message']}")
        return None
    except Exception as e:
        console.print(f"❌ Unexpected error: [bold red]{str(e)}[/bold red]")
        return None


def create_playground_bucket(s3_client):
    """Create a unique bucket for this session"""
    console.print("\n" + "="*60)
    console.print("📦 CREATING PLAYGROUND BUCKET")
    console.print("="*60)
    
    import random
    import string
    
    # Create a unique bucket name
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    bucket_name = f"iceberg-playground-{random_suffix}"
    
    try:
        # Create bucket
        console.print(f"🔧 Creating bucket: [bold]{bucket_name}[/bold]")
        s3_client.create_bucket(Bucket=bucket_name)
        
        console.print(f"✅ Created bucket: [green]{bucket_name}[/green]")
        console.print(f"   💡 Remember this bucket name for later steps")
        
        # Update environment variable for other scripts
        os.environ['PLAYGROUND_BUCKET'] = bucket_name
        
        return bucket_name
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            console.print(f"ℹ️  Bucket name [yellow]{bucket_name}[/yellow] already exists")
            console.print("   Trying a different name...")
            # Recursive retry with different suffix
            return create_playground_bucket(s3_client)
        else:
            console.print(f"❌ Failed to create bucket: {e}")
            return None
    except Exception as e:
        console.print(f"❌ Unexpected error creating bucket: {e}")
        return None


def test_playground_operations(s3_client, bucket_name):
    """Test basic operations on playground"""
    console.print("\n" + "="*60)
    console.print("✍️  TESTING PLAYGROUND OPERATIONS")
    console.print("="*60)
    
    test_key = f"test-{random.randint(1000, 9999)}/hello.txt"
    test_content = f"Hello from MinIO playground! Timestamp: {pd.Timestamp.now()}"
    
    try:
        # Test write
        console.print(f"📝 Writing test file: s3://{bucket_name}/{test_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content,
            ContentType='text/plain'
        )
        console.print("✅ Write test successful")
        
        # Test read
        console.print(f"📖 Reading test file...")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        console.print(f"✅ Read test successful: {len(content)} bytes")
        console.print(f"   Content preview: [dim]{content[:50]}...[/dim]")
        
        # Test list
        console.print(f"📋 Listing objects...")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        objects = response.get('Contents', [])
        console.print(f"✅ List test successful: found {len(objects)} objects")
        
        return True
        
    except Exception as e:
        console.print(f"❌ Playground operations failed: {e}")
        return False


def setup_playground_catalog_config(bucket_name):
    """Update catalog configuration to use playground bucket"""
    console.print("\n" + "="*60)
    console.print("⚙️  UPDATING CATALOG CONFIGURATION")
    console.print("="*60)
    
    # Update environment variables for catalog
    warehouse_url = f"s3://{bucket_name}/warehouse/"
    os.environ['PYICEBERG_CATALOG__PLAYGROUND__WAREHOUSE'] = warehouse_url
    
    console.print(f"✅ Updated warehouse location:")
    console.print(f"   [cyan]WAREHOUSE[/cyan] = [yellow]{warehouse_url}[/yellow]")
    
    # Create a simple .env file for playground
    playground_env = Path('.env.playground')
    env_content = f"""# Generated playground configuration
MINIO_ENDPOINT={os.getenv('MINIO_ENDPOINT')}
MINIO_ACCESS_KEY={os.getenv('MINIO_ACCESS_KEY')}
MINIO_SECRET_KEY={os.getenv('MINIO_SECRET_KEY')}
MINIO_REGION={os.getenv('MINIO_REGION')}

PYICEBERG_CATALOG__PLAYGROUND__TYPE=sql
PYICEBERG_CATALOG__PLAYGROUND__URI=sqlite:///playground-catalog.db
PYICEBERG_CATALOG__PLAYGROUND__WAREHOUSE={warehouse_url}
PYICEBERG_CATALOG__PLAYGROUND__S3__ENDPOINT={os.getenv('MINIO_ENDPOINT')}
PYICEBERG_CATALOG__PLAYGROUND__S3__ACCESS_KEY_ID={os.getenv('MINIO_ACCESS_KEY')}
PYICEBERG_CATALOG__PLAYGROUND__S3__SECRET_ACCESS_KEY={os.getenv('MINIO_SECRET_KEY')}
PYICEBERG_CATALOG__PLAYGROUND__S3__REGION={os.getenv('MINIO_REGION')}
PYICEBERG_CATALOG__PLAYGROUND__S3__PATH_STYLE_ACCESS=true

PLAYGROUND_BUCKET={bucket_name}
"""
    
    with open(playground_env, 'w') as f:
        f.write(env_content)
    
    console.print(f"✅ Created playground config: [green]{playground_env}[/green]")
    console.print("   Use this with: [cyan]cp .env.playground .env[/cyan]")


def main():
    """Main execution flow"""
    console.print("🎮 MinIO Playground Setup")
    console.print("=" * 60)
    
    # Display playground information
    display_playground_info()
    
    # Load playground environment
    if not load_playground_environment():
        console.print("❌ Cannot proceed without playground configuration")
        return False
    
    # Test connection
    s3_client = test_playground_connection()
    if not s3_client:
        console.print("❌ Cannot connect to playground")
        return False
    
    # Create unique bucket
    bucket_name = create_playground_bucket(s3_client)
    if not bucket_name:
        console.print("❌ Cannot create playground bucket")
        return False
    
    # Test basic operations
    if not test_playground_operations(s3_client, bucket_name):
        console.print("⚠️  Playground operations had issues")
    
    # Setup catalog configuration
    setup_playground_catalog_config(bucket_name)
    
    # Final instructions
    console.print("\n" + "="*60)
    console.print("🎯 PLAYGROUND SETUP COMPLETE")
    console.print("="*60)
    
    console.print(f"✅ Your playground bucket: [green]{bucket_name}[/green]")
    console.print("✅ Catalog configuration created")
    
    console.print("\n📝 Next steps:")
    console.print("1. Copy playground config: [cyan]cp .env.playground .env[/cyan]")
    console.print("2. Run catalog setup: [cyan]uv run src/02_catalog_setup.py[/cyan]")
    console.print("3. Continue with other tutorials")
    
    console.print("\n⚠️  Remember:")
    console.print("• Playground data is temporary")
    console.print("• Bucket will be cleaned up periodically") 
    console.print("• Use only for learning and testing")
    
    return True


if __name__ == "__main__":
    import random
    import pandas as pd  # For timestamp
    
    success = main()
    sys.exit(0 if success else 1)