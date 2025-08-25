#!/usr/bin/env python3
"""
Step 9: Monitoring and Observability Demo

This script demonstrates:
- MinIO metrics collection and monitoring
- Application-level observability patterns
- Distributed tracing for data operations
- Logging strategies and log aggregation
- Alerting rules and incident response
- Custom dashboards and visualization
"""

import json
import logging
import os
import random
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn

console = Console()

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, show_time=False, show_path=False),
        logging.FileHandler("minio_operations.log")
    ]
)

logger = logging.getLogger("minio_monitoring")

class OperationMetrics:
    """Simple metrics collector for demonstration"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, operation_name: str):
        """Start timing an operation"""
        self.start_times[operation_name] = time.time()
    
    def end_timer(self, operation_name: str, tags: Dict[str, str] = None):
        """End timing and record metric"""
        if operation_name in self.start_times:
            duration = time.time() - self.start_times[operation_name]
            self.record_metric(f"{operation_name}_duration", duration, tags or {})
            del self.start_times[operation_name]
            return duration
        return 0
    
    def record_metric(self, metric_name: str, value: float, tags: Dict[str, str] = None):
        """Record a metric value"""
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append({
            'timestamp': datetime.now(),
            'value': value,
            'tags': tags or {}
        })
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        summary = {}
        for metric_name, values in self.metrics.items():
            if values:
                numeric_values = [v['value'] for v in values]
                summary[metric_name] = {
                    'count': len(numeric_values),
                    'avg': sum(numeric_values) / len(numeric_values),
                    'min': min(numeric_values),
                    'max': max(numeric_values),
                    'latest': numeric_values[-1]
                }
        return summary

# Global metrics collector
metrics_collector = OperationMetrics()

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

@contextmanager
def monitored_operation(operation_name: str, tags: Dict[str, str] = None):
    """Context manager for monitoring operations"""
    logger.info(f"Starting operation: {operation_name}", extra={'tags': tags})
    metrics_collector.start_timer(operation_name)
    
    start_time = time.time()
    success = False
    
    try:
        yield
        success = True
    except Exception as e:
        logger.error(f"Operation failed: {operation_name}", exc_info=True, extra={'error': str(e), 'tags': tags})
        metrics_collector.record_metric(f"{operation_name}_errors", 1, tags)
        raise
    finally:
        duration = metrics_collector.end_timer(operation_name, tags)
        status = "success" if success else "error"
        logger.info(f"Completed operation: {operation_name}", extra={
            'duration': duration,
            'status': status,
            'tags': tags
        })

def get_monitored_s3_client():
    """Get S3 client with monitoring capabilities"""
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    region = os.getenv('MINIO_REGION', 'us-east-1')
    
    client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        use_ssl=endpoint.startswith('https'),
        verify=True if endpoint.startswith('https') else False
    )
    
    return client

def display_observability_overview():
    """Display observability concepts overview"""
    obs_panel = Panel.fit(
        """[bold cyan]Monitoring and Observability Architecture[/bold cyan]

🔍 [bold]Three Pillars of Observability:[/bold]
• [green]Metrics[/green] - Quantitative measurements over time
• [yellow]Logs[/yellow] - Discrete events and contextual information
• [blue]Traces[/blue] - Request flow through distributed systems

📊 [bold]Key Monitoring Areas:[/bold]
• [green]Infrastructure[/green] - CPU, memory, network, storage
• [yellow]Application[/yellow] - Response times, error rates, throughput
• [blue]Business[/blue] - User experience, data freshness, SLA compliance
• [magenta]Security[/magenta] - Access patterns, anomaly detection

🎯 [bold]MinIO Specific Metrics:[/bold]
• API request rates and latencies
• Object storage usage and growth
• Error rates and retry patterns
• Network bandwidth utilization
• Connection pool usage""",
        title="Observability Overview",
        border_style="cyan"
    )
    console.print(obs_panel)

def demonstrate_metrics_collection():
    """Demonstrate metrics collection patterns"""
    console.print("\n" + "="*60)
    console.print("📊 METRICS COLLECTION DEMO")
    console.print("="*60)
    
    client = get_monitored_s3_client()
    bucket_name = "iceberg-warehouse"
    
    # Simulate various operations with metrics
    operations = [
        ("list_buckets", lambda: client.list_buckets()),
        ("list_objects", lambda: client.list_objects_v2(Bucket=bucket_name, MaxKeys=10)),
        ("head_bucket", lambda: client.head_bucket(Bucket=bucket_name)),
    ]
    
    console.print("🔧 Performing monitored operations...")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        for op_name, op_func in operations:
            task = progress.add_task(f"Executing {op_name}...", total=1)
            
            # Perform operation multiple times to collect metrics
            for i in range(3):
                tags = {"operation": op_name, "iteration": str(i+1)}
                
                try:
                    with monitored_operation(op_name, tags):
                        result = op_func()
                        
                        # Record operation-specific metrics
                        if op_name == "list_objects" and "Contents" in result:
                            metrics_collector.record_metric(
                                "objects_returned", 
                                len(result["Contents"]), 
                                tags
                            )
                        
                        # Simulate network latency variation
                        time.sleep(random.uniform(0.01, 0.05))
                        
                except Exception as e:
                    logger.error(f"Operation {op_name} failed: {e}")
            
            progress.update(task, completed=1)
    
    # Display collected metrics
    console.print("\n📈 Collected metrics summary:")
    
    summary = metrics_collector.get_summary()
    metrics_table = Table()
    metrics_table.add_column("Metric", style="cyan")
    metrics_table.add_column("Count", style="green")
    metrics_table.add_column("Average", style="yellow")
    metrics_table.add_column("Min", style="blue")
    metrics_table.add_column("Max", style="magenta")
    
    for metric_name, stats in summary.items():
        metrics_table.add_row(
            metric_name,
            str(stats['count']),
            f"{stats['avg']:.3f}",
            f"{stats['min']:.3f}",
            f"{stats['max']:.3f}"
        )
    
    console.print(metrics_table)

def demonstrate_structured_logging():
    """Demonstrate structured logging patterns"""
    console.print("\n" + "="*60)
    console.print("📝 STRUCTURED LOGGING DEMO")
    console.print("="*60)
    
    logging_panel = Panel.fit(
        """[bold cyan]Structured Logging Best Practices[/bold cyan]

📋 [bold]Key Principles:[/bold]
• [green]Consistent Format[/green] - Use structured formats (JSON, key=value)
• [yellow]Contextual Information[/green] - Include request IDs, user context
• [blue]Appropriate Levels[/blue] - DEBUG, INFO, WARN, ERROR, CRITICAL
• [magenta]Correlation IDs[/magenta] - Track requests across services

🎯 [bold]What to Log:[/bold]
• All API calls with parameters and results
• Error conditions with full context
• Performance metrics and timing
• Security events and access patterns
• Configuration changes and deployments

⚡ [bold]Log Aggregation:[/bold]
• Centralized log collection (ELK, Fluentd)
• Searchable and queryable logs
• Real-time log streaming and analysis
• Long-term retention and archival""",
        title="Structured Logging",
        border_style="green"
    )
    console.print(logging_panel)
    
    # Demonstrate different log levels and structures
    console.print("📋 Generating structured log examples...")
    
    # Simulate different operations with structured logging
    operations = [
        {
            "operation": "create_table",
            "table_name": "monitoring_demo.events",
            "success": True,
            "duration": 0.245,
            "records_inserted": 1000
        },
        {
            "operation": "query_table", 
            "table_name": "monitoring_demo.events",
            "success": True,
            "duration": 0.156,
            "records_scanned": 5000,
            "records_returned": 50
        },
        {
            "operation": "backup_table",
            "table_name": "monitoring_demo.events", 
            "success": False,
            "duration": 2.345,
            "error": "Insufficient storage space",
            "error_code": "STORAGE_FULL"
        }
    ]
    
    # Generate sample log entries
    for op in operations:
        correlation_id = f"req_{random.randint(1000, 9999)}"
        
        if op["success"]:
            logger.info(
                f"Operation completed successfully: {op['operation']}",
                extra={
                    "correlation_id": correlation_id,
                    "operation": op["operation"],
                    "table_name": op["table_name"],
                    "duration_seconds": op["duration"],
                    "records_inserted": op.get("records_inserted"),
                    "records_scanned": op.get("records_scanned"),
                    "records_returned": op.get("records_returned")
                }
            )
        else:
            logger.error(
                f"Operation failed: {op['operation']}",
                extra={
                    "correlation_id": correlation_id,
                    "operation": op["operation"],
                    "table_name": op["table_name"], 
                    "duration_seconds": op["duration"],
                    "error_message": op["error"],
                    "error_code": op["error_code"]
                }
            )
    
    console.print("✅ Structured logs written to: [cyan]minio_operations.log[/cyan]")

def demonstrate_distributed_tracing():
    """Demonstrate distributed tracing concepts"""
    console.print("\n" + "="*60)
    console.print("🔗 DISTRIBUTED TRACING DEMO")
    console.print("="*60)
    
    tracing_panel = Panel.fit(
        """[bold cyan]Distributed Tracing in Data Pipelines[/bold cyan]

🎯 [bold]What is Distributed Tracing?[/bold]
• Track requests across multiple services and systems
• Understand end-to-end latency and bottlenecks
• Identify failure points in complex workflows
• Correlate logs and metrics across services

🔧 [bold]Tracing Components:[/bold]
• [green]Spans[/green] - Individual operations with timing
• [yellow]Traces[/yellow] - Collection of spans for one request
• [blue]Context[/blue] - Correlation information passed between services
• [magenta]Sampling[/magenta] - Control overhead vs observability

⚡ [bold]Data Pipeline Tracing:[/bold]
• ETL job execution across multiple stages
• Data quality checks and validations
• Cross-system data movement
• Schema evolution and migrations""",
        title="Distributed Tracing",
        border_style="blue"
    )
    console.print(tracing_panel)
    
    # Simulate a distributed data pipeline trace
    console.print("🔍 Simulating data pipeline trace:")
    
    trace_id = f"trace_{random.randint(10000, 99999)}"
    
    pipeline_stages = [
        {
            "stage": "data_ingestion",
            "service": "etl-service",
            "duration": 1.234,
            "records_processed": 10000,
            "status": "success"
        },
        {
            "stage": "data_validation",
            "service": "validation-service", 
            "duration": 0.567,
            "records_validated": 10000,
            "validation_errors": 5,
            "status": "success"
        },
        {
            "stage": "data_transformation",
            "service": "transform-service",
            "duration": 2.890,
            "records_input": 10000,
            "records_output": 9995,
            "status": "success"
        },
        {
            "stage": "iceberg_write",
            "service": "iceberg-writer",
            "duration": 0.456,
            "files_written": 3,
            "bytes_written": 15728640,
            "status": "success"
        },
        {
            "stage": "minio_upload",
            "service": "minio-client",
            "duration": 1.123, 
            "objects_uploaded": 3,
            "bytes_uploaded": 15728640,
            "status": "success"
        }
    ]
    
    trace_table = Table()
    trace_table.add_column("Stage", style="cyan")
    trace_table.add_column("Service", style="green") 
    trace_table.add_column("Duration", style="yellow")
    trace_table.add_column("Details", style="white", width=30)
    trace_table.add_column("Status", style="bold")
    
    total_duration = 0
    for stage in pipeline_stages:
        total_duration += stage["duration"]
        
        # Format details based on stage
        details = []
        for key, value in stage.items():
            if key not in ["stage", "service", "duration", "status"]:
                if isinstance(value, int) and value > 1000:
                    details.append(f"{key}: {value:,}")
                else:
                    details.append(f"{key}: {value}")
        
        status_text = "[green]✅ SUCCESS[/green]" if stage["status"] == "success" else "[red]❌ FAILED[/red]"
        
        trace_table.add_row(
            stage["stage"],
            stage["service"],
            f"{stage['duration']:.3f}s",
            "\n".join(details[:2]),  # Show first 2 details
            status_text
        )
    
    console.print(f"🔗 Trace ID: [cyan]{trace_id}[/cyan]")
    console.print(f"📊 Total Duration: [yellow]{total_duration:.3f}s[/yellow]")
    console.print(trace_table)

def demonstrate_alerting_rules():
    """Demonstrate alerting rules and thresholds"""
    console.print("\n" + "="*60)
    console.print("🚨 ALERTING RULES DEMO")
    console.print("="*60)
    
    alerting_panel = Panel.fit(
        """[bold cyan]Intelligent Alerting Strategy[/bold cyan]

⚠️  [bold]Alert Categories:[/bold]
• [red]Critical[/red] - Service down, data loss, security breaches
• [yellow]Warning[/yellow] - Performance degradation, approaching limits
• [blue]Info[/blue] - Deployments, configuration changes
• [green]Recovery[/green] - Issues resolved, services restored

🎯 [bold]Smart Alerting Principles:[/bold]
• Actionable alerts only - every alert should require action
• Context-rich notifications with runbook links
• De-duplication and alert grouping
• Escalation policies and on-call rotation

📊 [bold]MinIO Specific Alerts:[/bold]
• High error rates (> 1% for 5 minutes)
• Slow response times (P95 > 500ms)
• Storage capacity warnings (> 80% full)
• Failed backup or replication jobs""",
        title="Alerting Rules",
        border_style="red"
    )
    console.print(alerting_panel)
    
    # Simulate current system status and alerting
    current_metrics = {
        "error_rate": 0.3,           # 0.3% - OK
        "p95_latency": 145,          # 145ms - OK  
        "storage_usage": 85,         # 85% - WARNING
        "connection_pool_usage": 65, # 65% - OK
        "backup_age_hours": 26,      # 26 hours - WARNING
        "failed_requests": 3,        # 3 failed - OK
    }
    
    # Define alerting rules
    alerting_rules = [
        {
            "rule": "High Error Rate",
            "condition": "error_rate > 1.0",
            "severity": "critical",
            "current_value": current_metrics["error_rate"],
            "threshold": 1.0,
            "triggered": current_metrics["error_rate"] > 1.0
        },
        {
            "rule": "High Latency",
            "condition": "p95_latency > 200",
            "severity": "warning",
            "current_value": current_metrics["p95_latency"], 
            "threshold": 200,
            "triggered": current_metrics["p95_latency"] > 200
        },
        {
            "rule": "Storage Usage High",
            "condition": "storage_usage > 80",
            "severity": "warning", 
            "current_value": current_metrics["storage_usage"],
            "threshold": 80,
            "triggered": current_metrics["storage_usage"] > 80
        },
        {
            "rule": "Backup Delay",
            "condition": "backup_age_hours > 24",
            "severity": "warning",
            "current_value": current_metrics["backup_age_hours"],
            "threshold": 24,
            "triggered": current_metrics["backup_age_hours"] > 24
        },
        {
            "rule": "Connection Pool Exhaustion",
            "condition": "connection_pool_usage > 90",
            "severity": "critical",
            "current_value": current_metrics["connection_pool_usage"],
            "threshold": 90,
            "triggered": current_metrics["connection_pool_usage"] > 90
        }
    ]
    
    # Display alert status
    alert_table = Table()
    alert_table.add_column("Alert Rule", style="cyan")
    alert_table.add_column("Condition", style="white", width=20)
    alert_table.add_column("Current", style="green")
    alert_table.add_column("Threshold", style="yellow")
    alert_table.add_column("Status", style="bold")
    
    active_alerts = []
    
    for rule in alerting_rules:
        if rule["triggered"]:
            if rule["severity"] == "critical":
                status = "[red]🚨 CRITICAL[/red]"
            else:
                status = "[yellow]⚠️  WARNING[/yellow]"
            active_alerts.append(rule)
        else:
            status = "[green]✅ OK[/green]"
        
        # Format current value based on metric type
        current = rule["current_value"]
        if "rate" in rule["rule"].lower():
            current_str = f"{current}%"
        elif "latency" in rule["rule"].lower():
            current_str = f"{current}ms"
        elif "usage" in rule["rule"].lower():
            current_str = f"{current}%"
        elif "hours" in rule["rule"].lower():
            current_str = f"{current}h"
        else:
            current_str = str(current)
        
        alert_table.add_row(
            rule["rule"],
            rule["condition"],
            current_str,
            str(rule["threshold"]),
            status
        )
    
    console.print(alert_table)
    
    # Show active alerts with details
    if active_alerts:
        console.print(f"\n🚨 Active alerts: {len(active_alerts)}")
        
        for alert in active_alerts:
            severity_color = "red" if alert["severity"] == "critical" else "yellow"
            console.print(f"  [{severity_color}]•[/{severity_color}] {alert['rule']}: {alert['current_value']} (threshold: {alert['threshold']})")
    else:
        console.print("\n✅ [green]No active alerts - system healthy[/green]")

def create_monitoring_dashboard():
    """Create a sample monitoring dashboard layout"""
    console.print("\n" + "="*60)
    console.print("📊 MONITORING DASHBOARD")
    console.print("="*60)
    
    # Dashboard layout structure
    dashboard_tree = Tree("📊 [bold cyan]MinIO + Iceberg Monitoring Dashboard[/bold cyan]")
    
    # Infrastructure section
    infra_section = dashboard_tree.add("🏗️  [bold]Infrastructure Metrics[/bold]")
    infra_section.add("[green]CPU Usage[/green] - 45% (Target: < 70%)")
    infra_section.add("[green]Memory Usage[/green] - 62% (Target: < 80%)")
    infra_section.add("[green]Network I/O[/green] - 125 MB/s (Capacity: 1 GB/s)")
    infra_section.add("[yellow]Disk Usage[/yellow] - 85% (Target: < 80%)")
    
    # Application metrics
    app_section = dashboard_tree.add("📱 [bold]Application Metrics[/bold]")
    app_section.add("[green]Request Rate[/green] - 850 req/s")
    app_section.add("[green]P50 Latency[/green] - 45ms")
    app_section.add("[yellow]P95 Latency[/yellow] - 145ms (Target: < 100ms)")
    app_section.add("[green]Error Rate[/green] - 0.3%")
    
    # Business metrics
    business_section = dashboard_tree.add("💼 [bold]Business Metrics[/bold]") 
    business_section.add("[green]Tables Created[/green] - 12 (today)")
    business_section.add("[green]Data Ingested[/green] - 2.3 GB (today)")
    business_section.add("[green]Queries Executed[/green] - 1,234 (today)")
    business_section.add("[blue]Active Users[/blue] - 23 (current)")
    
    # Data quality
    quality_section = dashboard_tree.add("✅ [bold]Data Quality[/bold]")
    quality_section.add("[green]Schema Compliance[/green] - 99.8%")
    quality_section.add("[green]Data Freshness[/green] - 5 minutes (SLA: < 15 min)")
    quality_section.add("[yellow]Validation Failures[/yellow] - 3 (last hour)")
    quality_section.add("[green]Backup Success Rate[/green] - 100% (last 7 days)")
    
    console.print(dashboard_tree)
    
    # Sample alert summary
    console.print("\n🚨 Alert Summary:")
    alert_summary_table = Table()
    alert_summary_table.add_column("Severity", style="bold")
    alert_summary_table.add_column("Count", style="white")
    alert_summary_table.add_column("Latest", style="dim")
    
    alert_summary_table.add_row("[red]Critical[/red]", "0", "None")
    alert_summary_table.add_row("[yellow]Warning[/yellow]", "2", "Storage usage high (5 min ago)")
    alert_summary_table.add_row("[green]Info[/green]", "5", "Deployment completed (1 hour ago)")
    
    console.print(alert_summary_table)

def create_observability_checklist():
    """Create observability implementation checklist"""
    console.print("\n" + "="*60)
    console.print("✅ OBSERVABILITY IMPLEMENTATION CHECKLIST")
    console.print("="*60)
    
    checklist_sections = [
        ("Metrics Collection", [
            "Set up MinIO Prometheus metrics endpoint",
            "Configure application-level metrics (response times, errors)",
            "Implement business metrics tracking",
            "Set up metrics retention and storage (InfluxDB, Prometheus)"
        ]),
        ("Logging Strategy", [
            "Implement structured logging (JSON format)",
            "Add correlation IDs to track requests", 
            "Set up centralized log aggregation (ELK, Fluentd)",
            "Configure log retention policies"
        ]),
        ("Distributed Tracing", [
            "Add tracing instrumentation to data pipelines",
            "Implement context propagation between services",
            "Set up tracing backend (Jaeger, Zipkin)",
            "Configure sampling rates and retention"
        ]),
        ("Alerting & Dashboards", [
            "Define SLIs and SLOs for critical services",
            "Create actionable alerting rules",
            "Build monitoring dashboards for different audiences",
            "Set up on-call rotation and escalation policies"
        ]),
        ("Incident Response", [
            "Create runbooks for common issues",
            "Set up incident management process",
            "Implement automated remediation where possible",
            "Conduct regular post-incident reviews"
        ])
    ]
    
    for section, items in checklist_sections:
        console.print(f"\n[bold cyan]{section}:[/bold cyan]")
        for item in items:
            console.print(f"  □ {item}")

def main():
    """Main execution flow"""
    console.print("📊 MinIO Monitoring and Observability Demo")
    console.print("=" * 60)
    
    # Load environment
    if not load_environment():
        console.print("❌ Cannot proceed without environment configuration")
        return False
    
    # Display observability overview
    display_observability_overview()
    
    # Demonstrate metrics collection
    demonstrate_metrics_collection()
    
    # Show structured logging
    demonstrate_structured_logging()
    
    # Distributed tracing concepts
    demonstrate_distributed_tracing()
    
    # Alerting rules and monitoring
    demonstrate_alerting_rules()
    
    # Create sample dashboard
    create_monitoring_dashboard()
    
    # Implementation checklist
    create_observability_checklist()
    
    # Summary
    console.print("\n" + "="*60)
    console.print("🎯 MONITORING AND OBSERVABILITY DEMO COMPLETE")
    console.print("="*60)
    
    console.print("✅ [green]What we covered:[/green]")
    console.print("• Metrics collection and monitoring patterns")
    console.print("• Structured logging and log aggregation")
    console.print("• Distributed tracing for data pipelines")
    console.print("• Intelligent alerting rules and thresholds")
    console.print("• Monitoring dashboard design and layout")
    
    console.print("\n📝 [yellow]Production recommendations:[/yellow]")
    console.print("• Implement comprehensive metrics from day one")
    console.print("• Use structured logging with correlation IDs")
    console.print("• Set up distributed tracing for complex workflows")
    console.print("• Create actionable alerts with clear runbooks")
    console.print("• Build role-specific dashboards for different teams")
    console.print("• Regular monitoring and alerting rule reviews")
    
    console.print(f"\n📄 [cyan]Logs written to:[/cyan] minio_operations.log")
    console.print(f"📊 [cyan]Metrics collected:[/cyan] {len(metrics_collector.metrics)} different metrics")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)