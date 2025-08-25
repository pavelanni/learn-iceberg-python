#!/usr/bin/env python3
"""
Step 6: Multi-User Access and Security Demo

This script demonstrates:
- IAM user creation and policy management
- Secure credential distribution patterns
- Bucket policies for multi-user scenarios
- Role-based access control (RBAC) patterns
- Audit logging and access monitoring
- Temporary credential workflows
"""

import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
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

def get_minio_admin_client():
    """Get MinIO admin client for user management"""
    try:
        endpoint = os.getenv('MINIO_ENDPOINT')
        access_key = os.getenv('MINIO_ACCESS_KEY')  
        secret_key = os.getenv('MINIO_SECRET_KEY')
        region = os.getenv('MINIO_REGION', 'us-east-1')
        
        # For local MinIO, we need to use the admin API
        # Note: This requires minio-admin package or direct API calls
        admin_client = boto3.client(
            'iam',  # MinIO supports IAM-compatible API
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=endpoint.startswith('https'),
            verify=True if endpoint.startswith('https') else False
        )
        
        return admin_client
        
    except Exception as e:
        console.print(f"❌ Failed to create admin client: {e}")
        return None

def get_s3_client():
    """Get standard S3 client"""
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

def display_security_overview():
    """Display security concepts overview"""
    security_panel = Panel.fit(
        """[bold cyan]Multi-User Security Architecture[/bold cyan]

🏗️  [bold]Key Components:[/bold]
• [green]IAM Users[/green] - Individual identities with credentials
• [yellow]Policies[/yellow] - JSON documents defining permissions
• [blue]Roles[/blue] - Temporary identities with assumed permissions
• [magenta]Groups[/magenta] - Collections of users with shared permissions

🔐 [bold]Security Principles:[/bold]
• [green]Principle of Least Privilege[/green] - Minimal necessary permissions
• [yellow]Defense in Depth[/yellow] - Multiple security layers
• [blue]Separation of Duties[/blue] - No single user has excessive access
• [magenta]Regular Rotation[/magenta] - Periodic credential updates

🎯 [bold]Use Cases We'll Demonstrate:[/bold]
• Data scientists with read-only access to specific tables
• ETL engineers with write access to staging areas
• Administrators with full bucket management
• Applications using temporary credentials""",
        title="Security Architecture",
        border_style="cyan"
    )
    console.print(security_panel)

def create_user_policies():
    """Create IAM policies for different user roles"""
    console.print("\n" + "="*60)
    console.print("📋 CREATING IAM POLICIES")
    console.print("="*60)
    
    policies = {
        "data-scientist-readonly": {
            "description": "Read-only access to specific Iceberg tables",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::iceberg-warehouse/tables/*",
                            "arn:aws:s3:::iceberg-warehouse/metadata/*"
                        ],
                        "Condition": {
                            "StringLike": {
                                "s3:prefix": [
                                    "tables/analytics/*",
                                    "metadata/analytics/*"
                                ]
                            }
                        }
                    }
                ]
            }
        },
        "etl-engineer": {
            "description": "Read/write access to staging and ETL areas",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::iceberg-warehouse/staging/*",
                            "arn:aws:s3:::iceberg-warehouse/etl/*"
                        ]
                    },
                    {
                        "Effect": "Allow", 
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::iceberg-warehouse/tables/*",
                            "arn:aws:s3:::iceberg-warehouse/metadata/*"
                        ]
                    }
                ]
            }
        },
        "bucket-admin": {
            "description": "Full administrative access to buckets",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": [
                            "arn:aws:s3:::iceberg-warehouse",
                            "arn:aws:s3:::iceberg-warehouse/*"
                        ]
                    }
                ]
            }
        }
    }
    
    # Display policies in a table
    policy_table = Table()
    policy_table.add_column("Role", style="cyan", width=20)
    policy_table.add_column("Description", style="green", width=30)
    policy_table.add_column("Key Permissions", style="yellow", width=25)
    
    for policy_name, policy_data in policies.items():
        permissions = []
        for statement in policy_data["policy"]["Statement"]:
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            permissions.extend(actions[:2])  # Show first 2 actions
        
        policy_table.add_row(
            policy_name,
            policy_data["description"],
            "\n".join(permissions)
        )
    
    console.print(policy_table)
    return policies

def simulate_user_management(s3_client, policies):
    """Simulate user creation and policy attachment"""
    console.print("\n" + "="*60)
    console.print("👥 SIMULATING USER MANAGEMENT")
    console.print("="*60)
    
    console.print("ℹ️  [yellow]Note: This demo simulates user management concepts[/yellow]")
    console.print("   In production, use proper IAM/LDAP integration")
    
    # Simulate user scenarios
    users = [
        {
            "username": "alice-scientist",
            "role": "data-scientist-readonly",
            "scenario": "Data scientist analyzing sales data"
        },
        {
            "username": "bob-engineer", 
            "role": "etl-engineer",
            "scenario": "ETL engineer processing daily batches"
        },
        {
            "username": "carol-admin",
            "role": "bucket-admin", 
            "scenario": "Operations admin managing infrastructure"
        }
    ]
    
    user_table = Table()
    user_table.add_column("User", style="cyan")
    user_table.add_column("Role", style="green") 
    user_table.add_column("Scenario", style="yellow")
    user_table.add_column("Status", style="magenta")
    
    for user in users:
        # Simulate user creation
        console.print(f"👤 Creating user: [cyan]{user['username']}[/cyan]")
        time.sleep(0.5)  # Simulate API delay
        
        # Simulate policy attachment
        console.print(f"📎 Attaching policy: [green]{user['role']}[/green]")
        
        user_table.add_row(
            user['username'],
            user['role'],
            user['scenario'],
            "✅ Active"
        )
    
    console.print(user_table)

def demonstrate_access_patterns(s3_client):
    """Demonstrate different access patterns"""
    console.print("\n" + "="*60)
    console.print("🔐 ACCESS PATTERN DEMONSTRATIONS") 
    console.print("="*60)
    
    bucket_name = "iceberg-warehouse"
    
    # Create test structure
    test_objects = [
        "tables/analytics/sales/data/part-001.parquet",
        "tables/analytics/users/data/part-001.parquet", 
        "staging/raw-data/2024-01-15/data.json",
        "etl/processed/sales-summary/part-001.parquet",
        "metadata/analytics/sales/v1.metadata.json"
    ]
    
    console.print("🏗️  Setting up test data structure...")
    
    for obj_key in test_objects:
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=obj_key,
                Body=f"Test data for {obj_key}",
                ContentType="application/json" if obj_key.endswith('.json') else "application/octet-stream"
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchBucket':
                console.print(f"⚠️  Could not create {obj_key}: {e}")
    
    # Demonstrate access scenarios
    scenarios = [
        {
            "user": "alice-scientist",
            "action": "Read sales analytics data",
            "path": "tables/analytics/sales/",
            "allowed": True,
            "explanation": "Data scientist has read access to analytics tables"
        },
        {
            "user": "alice-scientist", 
            "action": "Write to staging area",
            "path": "staging/raw-data/",
            "allowed": False,
            "explanation": "Data scientist doesn't have write permissions"
        },
        {
            "user": "bob-engineer",
            "action": "Process staging data",
            "path": "staging/raw-data/", 
            "allowed": True,
            "explanation": "ETL engineer can read/write staging areas"
        },
        {
            "user": "bob-engineer",
            "action": "Write to production tables",
            "path": "tables/analytics/sales/",
            "allowed": False, 
            "explanation": "ETL engineer limited to staging/etl areas"
        },
        {
            "user": "carol-admin",
            "action": "Full bucket access",
            "path": "*",
            "allowed": True,
            "explanation": "Admin has unrestricted access"
        }
    ]
    
    access_table = Table()
    access_table.add_column("User", style="cyan")
    access_table.add_column("Action", style="white")
    access_table.add_column("Path", style="blue") 
    access_table.add_column("Result", style="bold")
    access_table.add_column("Explanation", style="dim")
    
    for scenario in scenarios:
        result_style = "[green]✅ ALLOW[/green]" if scenario["allowed"] else "[red]❌ DENY[/red]"
        
        access_table.add_row(
            scenario["user"],
            scenario["action"], 
            scenario["path"],
            result_style,
            scenario["explanation"]
        )
    
    console.print(access_table)

def demonstrate_temporary_credentials():
    """Demonstrate temporary credential patterns"""
    console.print("\n" + "="*60)
    console.print("⏰ TEMPORARY CREDENTIALS DEMO")
    console.print("="*60)
    
    temp_creds_info = Panel.fit(
        """[bold cyan]Temporary Credentials (STS)[/bold cyan]

🎫 [bold]What are Temporary Credentials?[/bold]
• Time-limited access credentials (15 min - 12 hours)
• Automatically expire, reducing security risk
• Can be scoped to specific resources and actions
• Ideal for applications and cross-account access

🔄 [bold]Common Patterns:[/bold]
• [green]Application Roles[/green] - Apps assume roles with specific permissions  
• [yellow]Cross-Account Access[/yellow] - Temporary access to external resources
• [blue]User Sessions[/blue] - Web apps with user-specific permissions
• [magenta]ETL Jobs[/magenta] - Batch jobs with job-specific credentials

⚡ [bold]Benefits:[/bold]
• No long-term credentials to manage
• Automatic expiration reduces breach impact
• Fine-grained permission scoping
• Audit trail of credential usage""",
        title="Temporary Credentials",
        border_style="yellow"
    )
    console.print(temp_creds_info)
    
    # Simulate STS token generation
    console.print("🎯 Simulating STS token generation...")
    
    # Mock temporary credentials (in real scenario, these come from STS)
    temp_credentials = {
        "AccessKeyId": "ASIA" + "X" * 16,  # STS credentials start with ASIA
        "SecretAccessKey": "x" * 40,
        "SessionToken": "y" * 100,
        "Expiration": "2024-01-15T14:30:00Z"
    }
    
    temp_table = Table()
    temp_table.add_column("Credential Type", style="cyan")
    temp_table.add_column("Value", style="green")
    temp_table.add_column("Notes", style="yellow")
    
    temp_table.add_row(
        "Access Key ID",
        temp_credentials["AccessKeyId"][:10] + "...",
        "Starts with 'ASIA' for temporary credentials"
    )
    temp_table.add_row(
        "Secret Access Key", 
        temp_credentials["SecretAccessKey"][:10] + "...",
        "Same length as permanent credentials"
    )
    temp_table.add_row(
        "Session Token",
        temp_credentials["SessionToken"][:20] + "...",
        "Additional token required for STS credentials"
    )
    temp_table.add_row(
        "Expiration",
        temp_credentials["Expiration"],
        "Credentials automatically invalid after this time"
    )
    
    console.print(temp_table)

def demonstrate_audit_logging():
    """Demonstrate audit logging patterns"""
    console.print("\n" + "="*60)
    console.print("📊 AUDIT LOGGING AND MONITORING")
    console.print("="*60)
    
    audit_panel = Panel.fit(
        """[bold cyan]Audit Logging Strategy[/bold cyan]

📝 [bold]What to Log:[/bold]
• All API calls (successful and failed)
• User authentication events  
• Permission changes and policy updates
• Data access patterns and anomalies
• Credential usage and rotation events

🔍 [bold]Key Metrics to Monitor:[/bold]
• [green]Access Frequency[/green] - Unusual access patterns
• [yellow]Failed Requests[/yellow] - Potential security probes
• [blue]Privilege Escalation[/blue] - Users gaining new permissions
• [magenta]Data Exfiltration[/magenta] - Large download volumes

⚡ [bold]Monitoring Tools:[/bold]
• MinIO built-in audit logs
• CloudTrail (for AWS S3)
• Prometheus metrics
• Grafana dashboards
• Custom alerting systems""",
        title="Audit and Monitoring",
        border_style="green"
    )
    console.print(audit_panel)
    
    # Sample audit log entries
    console.print("📋 Sample audit log entries:")
    
    audit_logs = [
        {
            "timestamp": "2024-01-15T10:15:30Z",
            "user": "alice-scientist",
            "action": "s3:GetObject", 
            "resource": "iceberg-warehouse/tables/analytics/sales/data/part-001.parquet",
            "result": "SUCCESS",
            "ip": "10.0.1.45"
        },
        {
            "timestamp": "2024-01-15T10:16:12Z",
            "user": "bob-engineer",
            "action": "s3:PutObject",
            "resource": "iceberg-warehouse/staging/raw-data/batch-001.json", 
            "result": "SUCCESS",
            "ip": "10.0.2.23"
        },
        {
            "timestamp": "2024-01-15T10:17:45Z",
            "user": "alice-scientist",
            "action": "s3:PutObject",
            "resource": "iceberg-warehouse/tables/analytics/sales/data/new-part.parquet",
            "result": "ACCESS_DENIED",
            "ip": "10.0.1.45"
        }
    ]
    
    audit_table = Table()
    audit_table.add_column("Time", style="dim")
    audit_table.add_column("User", style="cyan")
    audit_table.add_column("Action", style="white")
    audit_table.add_column("Resource", style="blue", max_width=30)
    audit_table.add_column("Result", style="bold")
    
    for log in audit_logs:
        result_style = "[green]SUCCESS[/green]" if log["result"] == "SUCCESS" else "[red]ACCESS_DENIED[/red]"
        
        audit_table.add_row(
            log["timestamp"].split("T")[1][:8],  # Just show time
            log["user"],
            log["action"],
            log["resource"].split("/")[-1],  # Just filename
            result_style
        )
    
    console.print(audit_table)

def demonstrate_security_best_practices():
    """Display security best practices"""
    console.print("\n" + "="*60)
    console.print("🛡️  SECURITY BEST PRACTICES")
    console.print("="*60)
    
    # Create a tree structure for best practices
    practices_tree = Tree("🔐 [bold cyan]Security Best Practices[/bold cyan]")
    
    # Credential Management
    creds_branch = practices_tree.add("💳 [bold]Credential Management[/bold]")
    creds_branch.add("[green]✅[/green] Use environment variables, never hardcode")
    creds_branch.add("[green]✅[/green] Rotate credentials regularly (30-90 days)")
    creds_branch.add("[green]✅[/green] Use temporary credentials when possible")
    creds_branch.add("[red]❌[/red] Never commit credentials to version control")
    
    # Access Control
    access_branch = practices_tree.add("🎯 [bold]Access Control[/bold]") 
    access_branch.add("[green]✅[/green] Principle of least privilege")
    access_branch.add("[green]✅[/green] Regular access reviews and cleanup")
    access_branch.add("[green]✅[/green] Use groups and roles, not direct user policies")
    access_branch.add("[red]❌[/red] Avoid wildcard permissions in production")
    
    # Network Security
    network_branch = practices_tree.add("🌐 [bold]Network Security[/bold]")
    network_branch.add("[green]✅[/green] Use TLS/SSL for all connections")
    network_branch.add("[green]✅[/green] Restrict access by IP/VPC when possible")
    network_branch.add("[green]✅[/green] Use private endpoints for internal traffic")
    network_branch.add("[red]❌[/red] Never disable SSL verification in production")
    
    # Monitoring
    monitor_branch = practices_tree.add("📊 [bold]Monitoring & Compliance[/bold]")
    monitor_branch.add("[green]✅[/green] Enable comprehensive audit logging")
    monitor_branch.add("[green]✅[/green] Set up alerts for unusual access patterns")
    monitor_branch.add("[green]✅[/green] Regular security audits and penetration testing")
    monitor_branch.add("[green]✅[/green] Document and test incident response procedures")
    
    console.print(practices_tree)

def create_security_checklist():
    """Create a security implementation checklist"""
    console.print("\n" + "="*60) 
    console.print("✅ IMPLEMENTATION CHECKLIST")
    console.print("="*60)
    
    checklist = [
        ("Environment Setup", [
            "Create separate .env files for dev/staging/prod",
            "Never commit .env files to version control",
            "Use secret management systems (Vault, AWS Secrets Manager)",
            "Document credential rotation procedures"
        ]),
        ("User Management", [
            "Create role-based policies following least privilege",
            "Set up user groups for easier management", 
            "Implement regular access reviews",
            "Document user onboarding/offboarding procedures"
        ]),
        ("Monitoring", [
            "Enable MinIO audit logging",
            "Set up log aggregation and analysis",
            "Create alerts for security events",
            "Regular review of access patterns and anomalies"
        ]),
        ("Network Security", [
            "Use TLS/SSL for all connections",
            "Implement network-level access controls",
            "Consider using private/internal endpoints",
            "Regular security scanning and updates"
        ])
    ]
    
    for category, items in checklist:
        console.print(f"\n[bold cyan]{category}:[/bold cyan]")
        for item in items:
            console.print(f"  □ {item}")

def main():
    """Main execution flow"""
    console.print("🔐 MinIO Multi-User Security Demo")
    console.print("=" * 60)
    
    # Load environment
    if not load_environment():
        console.print("❌ Cannot proceed without environment configuration")
        return False
    
    # Display security overview
    display_security_overview()
    
    # Get S3 client
    try:
        s3_client = get_s3_client()
    except Exception as e:
        console.print(f"❌ Failed to create S3 client: {e}")
        return False
    
    # Create policies
    policies = create_user_policies()
    
    # Simulate user management
    simulate_user_management(s3_client, policies)
    
    # Demonstrate access patterns
    demonstrate_access_patterns(s3_client)
    
    # Show temporary credentials
    demonstrate_temporary_credentials()
    
    # Show audit logging
    demonstrate_audit_logging()
    
    # Display best practices
    demonstrate_security_best_practices()
    
    # Implementation checklist
    create_security_checklist()
    
    # Summary
    console.print("\n" + "="*60)
    console.print("🎯 SECURITY DEMO COMPLETE")
    console.print("="*60)
    
    console.print("✅ [green]What we covered:[/green]")
    console.print("• IAM policies and role-based access control")
    console.print("• Multi-user access patterns and scenarios")  
    console.print("• Temporary credentials and STS tokens")
    console.print("• Audit logging and security monitoring")
    console.print("• Security best practices and implementation")
    
    console.print("\n📝 [yellow]Next steps for production:[/yellow]")
    console.print("• Integrate with your identity provider (LDAP/AD/OIDC)")
    console.print("• Set up automated credential rotation")
    console.print("• Implement monitoring and alerting")
    console.print("• Create security incident response procedures")
    console.print("• Regular security audits and penetration testing")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)