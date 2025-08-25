#!/usr/bin/env python3
"""
Step 1: MinIO Connection Test

This script demonstrates:
- Connecting to local MinIO instance
- Testing S3-compatible API functionality
- Creating buckets for Iceberg storage
- Verifying read/write permissions
- Debugging common connection issues
"""

import os
import sys
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

console = Console()


def load_environment():
    """Load environment variables from .env file"""
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
        console.print(f"✅ Loaded environment from: [green]{env_file}[/green]")
        return True
    else:
        console.print(f"ℹ️  No .env file found - using defaults")
        console.print("   💡 For production, create .env file with proper credentials")
        return False


def get_minio_client():
    """Create S3 client for local MinIO instance"""
    
    # Load environment configuration
    load_environment()
    
    # Get configuration from environment with sensible defaults
    endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    region = os.getenv('MINIO_REGION', 'us-east-1')
    
    console.print(f"🔗 Connecting to MinIO at: [bold blue]{endpoint_url}[/bold blue]")
    console.print(f"📂 Using access key: [bold yellow]{access_key[:8]}...[/bold yellow]")  # Mask for security
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        # For local development, disable SSL verification
        use_ssl=False,
        verify=False
    )


def test_basic_connectivity(s3_client):
    """Test basic S3 API connectivity"""
    console.print("\n" + "="*60)
    console.print("🧪 TESTING BASIC CONNECTIVITY")
    console.print("="*60)
    
    try:
        # List buckets (basic connectivity test)
        response = s3_client.list_buckets()
        
        console.print("✅ Successfully connected to MinIO!")
        
        # Display existing buckets
        buckets = response.get('Buckets', [])
        if buckets:
            table = Table(title="Existing Buckets")
            table.add_column("Bucket Name", style="cyan")
            table.add_column("Created", style="green")
            
            for bucket in buckets:
                table.add_row(
                    bucket['Name'],
                    bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')
                )
            console.print(table)
        else:
            console.print("ℹ️  No existing buckets found")
            
        return True
        
    except NoCredentialsError:
        console.print("❌ Authentication failed - check your credentials")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        console.print(f"❌ AWS Client Error: [bold red]{error_code}[/bold red]")
        console.print(f"   Message: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        console.print(f"❌ Unexpected error: [bold red]{str(e)}[/bold red]")
        console.print("\n💡 Common issues:")
        console.print("   • Is MinIO running? (podman ps or docker ps)")
        console.print("   • Check port mapping (9000 for API, 9001 for console)")
        console.print("   • Verify credentials (default: minioadmin/minioadmin)")
        return False


def create_iceberg_buckets(s3_client):
    """Create buckets needed for Iceberg storage"""
    console.print("\n" + "="*60)
    console.print("📦 CREATING ICEBERG BUCKETS")
    console.print("="*60)
    
    # Buckets we need for Iceberg
    required_buckets = [
        'iceberg-warehouse',  # Main data storage
        'iceberg-catalog',    # Catalog metadata (if using S3 catalog)
    ]
    
    created_buckets = []
    existing_buckets = []
    
    for bucket_name in required_buckets:
        try:
            # Check if bucket already exists
            s3_client.head_bucket(Bucket=bucket_name)
            existing_buckets.append(bucket_name)
            console.print(f"ℹ️  Bucket [yellow]{bucket_name}[/yellow] already exists")
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    created_buckets.append(bucket_name)
                    console.print(f"✅ Created bucket [green]{bucket_name}[/green]")
                except ClientError as create_error:
                    console.print(f"❌ Failed to create bucket {bucket_name}: {create_error}")
                    return False
            else:
                console.print(f"❌ Error checking bucket {bucket_name}: {e}")
                return False
    
    # Summary
    if created_buckets:
        console.print(f"\n🎉 Created {len(created_buckets)} new buckets: {created_buckets}")
    if existing_buckets:
        console.print(f"♻️  Found {len(existing_buckets)} existing buckets: {existing_buckets}")
    
    return True


def test_read_write_permissions(s3_client):
    """Test read/write permissions with a sample file"""
    console.print("\n" + "="*60)
    console.print("✍️  TESTING READ/WRITE PERMISSIONS")
    console.print("="*60)
    
    bucket_name = 'iceberg-warehouse'
    test_key = 'connection-test/sample.json'
    
    # Create test data
    test_data = {
        'message': 'Hello from PyIceberg + MinIO!',
        'timestamp': pd.Timestamp.now().isoformat(),
        'test_type': 'connection_verification'
    }
    
    try:
        # Test write
        console.print(f"📝 Writing test file to s3://{bucket_name}/{test_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=pd.Series(test_data).to_json(),
            ContentType='application/json'
        )
        console.print("✅ Write test successful")
        
        # Test read
        console.print(f"📖 Reading test file from s3://{bucket_name}/{test_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        console.print(f"✅ Read test successful: {len(content)} bytes")
        
        # Test list
        console.print(f"📋 Listing objects in s3://{bucket_name}/connection-test/")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, 
            Prefix='connection-test/'
        )
        objects = response.get('Contents', [])
        console.print(f"✅ List test successful: found {len(objects)} objects")
        
        # Clean up test file
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        console.print("🗑️  Cleaned up test file")
        
        return True
        
    except ClientError as e:
        console.print(f"❌ Permission test failed: {e}")
        console.print("\n💡 This might be a bucket policy issue")
        console.print("   For development, you can set bucket to public:")
        console.print(f"   mc policy set public local/{bucket_name}")
        return False
    except Exception as e:
        console.print(f"❌ Unexpected error during permission test: {e}")
        return False


def display_minio_info(s3_client):
    """Display useful MinIO instance information"""
    console.print("\n" + "="*60)
    console.print("ℹ️  MINIO INSTANCE INFORMATION")
    console.print("="*60)
    
    endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    console_url = endpoint_url.replace(':9000', ':9001')
    
    info_table = Table(title="MinIO Configuration")
    info_table.add_column("Property", style="cyan")
    info_table.add_column("Value", style="yellow")
    
    info_table.add_row("API Endpoint", endpoint_url)
    info_table.add_row("Console URL", console_url)
    info_table.add_row("Access Key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin'))
    info_table.add_row("Region", os.getenv('MINIO_REGION', 'us-east-1'))
    
    console.print(info_table)
    
    console.print(f"\n💡 Access MinIO Console: [link]{console_url}[/link]")
    console.print("   Use the same credentials to log in via web interface")


def main():
    """Main execution flow"""
    console.print("🚀 MinIO Connection Test for Apache Iceberg")
    console.print("=" * 60)
    
    # Test sequence
    tests_passed = 0
    total_tests = 4
    
    # 1. Get MinIO client
    try:
        s3_client = get_minio_client()
    except Exception as e:
        console.print(f"❌ Failed to create MinIO client: {e}")
        return False
    
    # 2. Test basic connectivity
    if test_basic_connectivity(s3_client):
        tests_passed += 1
    else:
        console.print("🛑 Basic connectivity failed - stopping here")
        return False
    
    # 3. Create necessary buckets
    if create_iceberg_buckets(s3_client):
        tests_passed += 1
    
    # 4. Test read/write permissions
    if test_read_write_permissions(s3_client):
        tests_passed += 1
    
    # 5. Display info (always runs)
    display_minio_info(s3_client)
    tests_passed += 1
    
    # Results summary
    console.print("\n" + "="*60)
    console.print("📊 TEST RESULTS SUMMARY")
    console.print("="*60)
    
    if tests_passed == total_tests:
        console.print(f"🎉 All tests passed! ({tests_passed}/{total_tests})")
        console.print("\n✅ Your MinIO instance is ready for Iceberg!")
        console.print("   Next step: Run [bold]02_catalog_setup.py[/bold]")
        return True
    else:
        console.print(f"⚠️  Some tests failed ({tests_passed}/{total_tests})")
        console.print("\n🔧 Please fix the issues above before proceeding")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)