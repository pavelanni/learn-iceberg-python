#!/usr/bin/env python3
"""
Step 5: Time Travel Queries with Iceberg

This script demonstrates:
- Querying table state at specific points in time
- Comparing data across different snapshots
- Understanding snapshot metadata and history
- Practical time travel use cases
- Rollback scenarios
"""

import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


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

def explore_snapshot_history(table):
    """Explore the complete history of table snapshots"""
    print("\n" + "="*60)
    print("COMPLETE SNAPSHOT HISTORY")
    print("="*60)

    snapshots = list(table.snapshots())
    print(f"Total snapshots: {len(snapshots)}")

    print("\nSnapshot Timeline:")
    print("-" * 80)
    print(f"{'#':<3} {'Snapshot ID':<20} {'Timestamp':<20} {'Operation':<12} {'Schema':<8} {'Records':<10}")
    print("-" * 80)

    for i, snapshot in enumerate(snapshots):
        timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
        operation = snapshot.summary.get('operation', 'unknown')
        schema_id = snapshot.schema_id if hasattr(snapshot, 'schema_id') else 'N/A'
        total_records = snapshot.summary.get('total-records', 'N/A')

        print(f"{i+1:<3} {str(snapshot.snapshot_id):<20} {timestamp.strftime('%Y-%m-%d %H:%M:%S'):<20} "
              f"{operation:<12} {schema_id:<8} {total_records:<10}")

    return snapshots

def query_snapshot_by_id(table, snapshot_id, description):
    """Query table state at a specific snapshot"""
    print(f"\n🕰️  Time Travel: {description}")
    print("-" * 50)
    print(f"Snapshot ID: {snapshot_id}")

    try:
        # Create a scan at specific snapshot
        historical_scan = table.scan(snapshot_id=snapshot_id)
        historical_data = historical_scan.to_arrow()
        df = historical_data.to_pandas()

        print(f"Records at this snapshot: {len(df)}")

        # Show basic stats
        if len(df) > 0:
            if 'timestamp' in df.columns:
                print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            if 'status_code' in df.columns:
                status_distribution = df['status_code'].value_counts().to_dict()
                print(f"Status codes: {status_distribution}")

            # Check for enhanced fields
            has_enhanced = 'user_country' in df.columns and df['user_country'].notna().any()
            print(f"Has enhanced fields: {has_enhanced}")

            if has_enhanced:
                country_counts = df['user_country'].value_counts().head(3).to_dict()
                print(f"Top countries: {country_counts}")

        return df

    except Exception as e:
        print(f"❌ Could not query snapshot: {e}")
        return None

def query_snapshot_by_timestamp(table, target_time, description):
    """Query table state as it existed at a specific time"""
    print(f"\n⏰ Time Travel by Timestamp: {description}")
    print("-" * 50)
    print(f"Target time: {target_time}")

    try:
        # Find the snapshot that was current at the target time
        snapshots = list(table.snapshots())
        target_snapshot = None

        for snapshot in snapshots:
            snapshot_time = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
            if snapshot_time <= target_time:
                target_snapshot = snapshot
            else:
                break

        if target_snapshot:
            print(f"Found snapshot: {target_snapshot.snapshot_id}")
            snapshot_time = datetime.fromtimestamp(target_snapshot.timestamp_ms / 1000)
            print(f"Snapshot time: {snapshot_time}")

            return query_snapshot_by_id(table, target_snapshot.snapshot_id, f"State at {target_time}")
        else:
            print("❌ No snapshot found for that time")
            return None

    except Exception as e:
        print(f"❌ Timestamp query failed: {e}")
        return None

def compare_snapshots(table, snapshot1_id, snapshot2_id):
    """Compare data between two snapshots"""
    print(f"\n🔍 Comparing Snapshots")
    print("-" * 30)
    print(f"Snapshot 1: {snapshot1_id}")
    print(f"Snapshot 2: {snapshot2_id}")

    try:
        # Get data from both snapshots
        data1 = table.scan(snapshot_id=snapshot1_id).to_arrow().to_pandas()
        data2 = table.scan(snapshot_id=snapshot2_id).to_arrow().to_pandas()

        print(f"\nRecord counts:")
        print(f"  Snapshot 1: {len(data1)} records")
        print(f"  Snapshot 2: {len(data2)} records")
        print(f"  Difference: +{len(data2) - len(data1)} records")

        # Check schema differences
        cols1 = set(data1.columns)
        cols2 = set(data2.columns)

        new_columns = cols2 - cols1
        if new_columns:
            print(f"\nNew columns in snapshot 2: {list(new_columns)}")

        # Compare overlapping data if possible
        common_cols = cols1 & cols2
        if 'status_code' in common_cols:
            print(f"\nStatus code distribution comparison:")
            dist1 = data1['status_code'].value_counts().sort_index()
            dist2 = data2['status_code'].value_counts().sort_index()

            comparison_df = pd.DataFrame({
                'Snapshot_1': dist1,
                'Snapshot_2': dist2
            }).fillna(0)
            comparison_df['Difference'] = comparison_df['Snapshot_2'] - comparison_df['Snapshot_1']
            print(comparison_df)

    except Exception as e:
        print(f"❌ Snapshot comparison failed: {e}")

def demonstrate_rollback_scenario(table, catalog):
    """Demonstrate how you could rollback to a previous state"""
    print(f"\n↩️  Rollback Scenario Demonstration")
    print("-" * 40)

    snapshots = list(table.snapshots())
    if len(snapshots) < 2:
        print("❌ Need at least 2 snapshots to demonstrate rollback")
        return

    current_snapshot = table.current_snapshot()
    previous_snapshot = snapshots[-2]  # Second to last snapshot

    print(f"Current snapshot: {current_snapshot.snapshot_id}")
    print(f"Previous snapshot: {previous_snapshot.snapshot_id}")
    print("\nIn a real scenario, you could:")
    print("1. Create a new table from a previous snapshot")
    print("2. Or use snapshot-specific queries for analysis")
    print("3. Or cherry-pick specific data from historical snapshots")

    # Show how to query the previous state
    print(f"\n📊 Data in previous snapshot:")
    try:
        prev_data = table.scan(snapshot_id=previous_snapshot.snapshot_id).to_arrow().to_pandas()
        print(f"   Records: {len(prev_data)}")

        curr_data = table.scan().to_arrow().to_pandas()
        print(f"   Current records: {len(curr_data)}")
        print(f"   Difference: {len(curr_data) - len(prev_data)} records")

    except Exception as e:
        print(f"   Could not compare: {e}")

def advanced_time_travel_queries(table):
    """Advanced time travel query patterns"""
    print(f"\n" + "="*60)
    print("ADVANCED TIME TRAVEL PATTERNS")
    print("="*60)

    snapshots = list(table.snapshots())

    print("🔍 Use Cases for Time Travel:")
    print()
    print("1. **Data Quality Investigation**")
    print("   'When did this bad data first appear?'")
    print("   → Query each snapshot until you find the introduction point")
    print()
    print("2. **Regulatory Compliance**")
    print("   'What did our customer data look like on audit date X?'")
    print("   → Query snapshot active at specific timestamp")
    print()
    print("3. **A/B Test Analysis**")
    print("   'Compare metrics before/after feature launch'")
    print("   → Query snapshots from different time periods")
    print()
    print("4. **Rollback Analysis**")
    print("   'Which records would we lose if we rollback?'")
    print("   → Compare current vs target snapshot")
    print()
    print("5. **Change Impact Analysis**")
    print("   'How did schema evolution affect our data?'")
    print("   → Compare snapshots across schema changes")

    if len(snapshots) >= 3:
        print(f"\n📈 Example: Growth Analysis")
        print("-" * 30)

        # Show growth over snapshots
        growth_data = []
        for i, snapshot in enumerate(snapshots):
            try:
                data = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().to_pandas()
                timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)

                growth_data.append({
                    'snapshot': i + 1,
                    'timestamp': timestamp,
                    'records': len(data),
                    'schema_id': getattr(snapshot, 'schema_id', 'N/A')
                })
            except:
                pass

        if growth_data:
            growth_df = pd.DataFrame(growth_data)
            print("Table growth over time:")
            print(growth_df.to_string(index=False))

def practical_time_travel_examples(table):
    """Show practical examples of time travel queries"""
    print(f"\n" + "="*60)
    print("PRACTICAL TIME TRAVEL EXAMPLES")
    print("="*60)

    snapshots = list(table.snapshots())

    if len(snapshots) >= 2:
        # Example 1: "Show me the data as it was 2 hours ago"
        now = datetime.now()
        two_hours_ago = now - timedelta(hours=2)

        print(f"🕐 Example 1: 'Show me data as it was 2 hours ago'")
        query_snapshot_by_timestamp(table, two_hours_ago, "2 hours ago")

        # Example 2: "Compare first vs current state"
        print(f"\n🔄 Example 2: 'Compare first snapshot vs current'")
        first_snapshot = snapshots[0]
        current_snapshot = snapshots[-1]
        compare_snapshots(table, first_snapshot.snapshot_id, current_snapshot.snapshot_id)

        # Example 3: "What changed between schema versions?"
        schema_0_snapshots = [s for s in snapshots if getattr(s, 'schema_id', 0) == 0]
        schema_2_snapshots = [s for s in snapshots if getattr(s, 'schema_id', 0) == 2]

        if schema_0_snapshots and schema_2_snapshots:
            print(f"\n🔄 Example 3: 'Schema evolution impact'")
            compare_snapshots(table, schema_0_snapshots[-1].snapshot_id, schema_2_snapshots[0].snapshot_id)

def main():
    """Main execution flow"""
    print("🕰️  Exploring Iceberg Time Travel Capabilities")
    print("-" * 60)

    # Load existing table
    catalog = get_catalog()
    table = catalog.load_table("web_logs.access_logs")

    # Explore snapshot history
    snapshots = explore_snapshot_history(table)

    if len(snapshots) == 0:
        print("❌ No snapshots found. Please run previous scripts first.")
        return

    # Query specific snapshots
    if len(snapshots) >= 1:
        first_snapshot = snapshots[0]
        query_snapshot_by_id(table, first_snapshot.snapshot_id, "Very First Data Load")

    if len(snapshots) >= 3:
        middle_snapshot = snapshots[len(snapshots)//2]
        query_snapshot_by_id(table, middle_snapshot.snapshot_id, "Middle Snapshot")

    # Query by timestamp
    practical_time_travel_examples(table)

    # Demonstrate rollback scenario
    demonstrate_rollback_scenario(table, catalog)

    # Advanced patterns
    advanced_time_travel_queries(table)

    # Show the power of time travel
    demonstrate_time_travel_power()

    print("\n🎉 Time travel exploration complete!")
    print("\nKey concepts learned:")
    print("- ✅ Every table change creates an immutable snapshot")
    print("- ✅ Query any historical state instantly")
    print("- ✅ Compare data across time periods")
    print("- ✅ Audit trail built into the table format")
    print("- ✅ Rollback capabilities without data loss")

    print("\nNext steps:")
    print("- Explore partitioning for large-scale data")
    print("- Try integrating with MinIO object storage")
    print("- Build real streaming data pipelines")

def demonstrate_time_travel_power():
    """Show why time travel is revolutionary"""
    print(f"\n" + "="*60)
    print("WHY TIME TRAVEL IS REVOLUTIONARY")
    print("="*60)

    print("🚀 Traditional Data Systems:")
    print("   ❌ 'We need last week's report' → Hope you have backups")
    print("   ❌ 'When did this bad data appear?' → Check logs, maybe")
    print("   ❌ 'Rollback that ETL job' → Complex, risky, expensive")
    print("   ❌ 'Compare before/after deployment' → Manual process")
    print()
    print("⚡ Iceberg + Time Travel:")
    print("   ✅ 'Show data as of last Tuesday' → One query")
    print("   ✅ 'When did this appear?' → Binary search through snapshots")
    print("   ✅ 'Rollback to snapshot X' → Metadata operation, instant")
    print("   ✅ 'Compare any two points in time' → Built-in capability")
    print()
    print("🏭 Real-World Applications:")
    print("   • **Financial Reconciliation**: 'What was the balance at market close?'")
    print("   • **Compliance Reporting**: 'Recreate the report as filed last quarter'")
    print("   • **Incident Investigation**: 'What changed between these two deployments?'")
    print("   • **Data Quality**: 'When did these outliers first appear?'")
    print("   • **A/B Testing**: 'Compare metrics before/after feature flag change'")
    print("   • **Customer Support**: 'What did the customer see at time X?'")
    print()
    print("🔮 The Git Analogy:")
    print("   Git for Code     │  Iceberg for Data")
    print("   ─────────────────┼──────────────────")
    print("   git log          │  snapshots()")
    print("   git checkout X   │  scan(snapshot_id=X)")
    print("   git diff A B     │  compare snapshots")
    print("   git revert       │  rollback to snapshot")
    print("   git blame        │  audit trail")

def demonstrate_duckdb_time_travel(table):
    """Show time travel with DuckDB queries"""
    print(f"\n" + "="*60)
    print("TIME TRAVEL WITH DUCKDB")
    print("="*60)

    snapshots = list(table.snapshots())

    if len(snapshots) < 2:
        print("Need at least 2 snapshots for comparison")
        return

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        conn.execute("SET unsafe_enable_version_guessing = true")

        # Note: DuckDB's Iceberg support for snapshot-specific queries
        # may be limited, so we'll demonstrate the concept
        table_location = table.location()

        print("🔍 Time Travel Query Patterns:")
        print()
        print("-- Current state")
        print(f"SELECT COUNT(*) FROM iceberg_scan('{table_location}');")

        print()
        print("-- Historical state (conceptual - exact syntax varies by engine)")
        print(f"SELECT COUNT(*) FROM iceberg_scan('{table_location}') FOR SNAPSHOT {snapshots[0].snapshot_id};")

        # Try basic current query
        conn.execute(f"CREATE VIEW current_logs AS SELECT * FROM iceberg_scan('{table_location}')")

        result = conn.execute("SELECT COUNT(*) as count FROM current_logs").fetchone()
        if result:
            current_count = result[0]
            print(f"\n📊 Current record count: {current_count}")
        else:
            print(f"\n📊 Could not get current record count")
            current_count = "unknown"

        # Show evolution of record counts
        print(f"\n📈 Table Growth Over Time:")
        for i, snapshot in enumerate(snapshots):
            timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
            records = snapshot.summary.get('total-records', 'unknown')
            print(f"   Snapshot {i+1} ({timestamp.strftime('%H:%M:%S')}): {records} records")

    except Exception as e:
        print(f"DuckDB time travel demo failed: {e}")
        print("(DuckDB's snapshot support varies by version)")
    finally:
        conn.close()

def time_travel_use_case_simulation(table):
    """Simulate real-world time travel use cases"""
    print(f"\n" + "="*60)
    print("REAL-WORLD USE CASE SIMULATION")
    print("="*60)

    snapshots = list(table.snapshots())

    print("🎭 Scenario: 'Bad Data Investigation'")
    print("-" * 40)
    print("Story: You notice unusual traffic patterns in today's dashboard.")
    print("Question: When did this pattern first appear?")
    print()

    # Simulate investigating each snapshot
    for i, snapshot in enumerate(snapshots[-3:]):  # Check last 3 snapshots
        print(f"🔍 Investigating Snapshot {len(snapshots)-2+i}:")

        try:
            data = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().to_pandas()
            timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)

            # Look for our "suspicious" late-arriving data
            if 'ip_address' in data.columns:
                suspicious_count = len(data[data['ip_address'] == '192.168.1.200'])
                print(f"   Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   Suspicious IPs: {suspicious_count}")

                if suspicious_count > 0:
                    print("   🚨 Found it! This is when the unusual pattern started.")
                    break
                else:
                    print("   ✅ Clean data at this point")

        except Exception as e:
            print(f"   ❌ Could not analyze: {e}")

    print()
    print("💡 Time travel enabled:")
    print("   - Fast root cause analysis")
    print("   - No need to search through backups")
    print("   - Precise identification of when issues started")
    print("   - Ability to understand data lineage")

if __name__ == "__main__":
    main()