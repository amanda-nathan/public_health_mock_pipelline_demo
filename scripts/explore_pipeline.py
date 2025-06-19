#!/usr/bin/env python3
"""
Explore the working data pipeline - show data, logs, and demonstrate functionality
"""
import os
import sys
import snowflake.connector
from datetime import datetime

def connect_to_snowflake(role='DATA_ENGINEER_ROLE'):
    """Create Snowflake connection with specified role"""
    return snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        role=role,
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'DEV_WH'),
        database='PUBLIC_HEALTH_MODERNIZATION_DEMO'
    )

def execute_query(cursor, query, description=""):
    """Execute a query and return results"""
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        if description:
            print(f"✅ {description}")
        return result
    except Exception as e:
        if description:
            print(f"❌ {description}: {str(e)[:150]}")
        return None

def show_pipeline_execution_log(cursor):
    """Show recent pipeline executions"""
    print("\n📋 Recent Pipeline Executions:")
    print("=" * 80)
    
    result = execute_query(cursor, """
        SELECT 
            procedure_name,
            execution_status,
            execution_start,
            execution_end,
            rows_processed,
            DATEDIFF(second, execution_start, execution_end) as duration_seconds
        FROM logging.pipeline_execution_log 
        ORDER BY execution_start DESC 
        LIMIT 10
    """)
    
    if result:
        print(f"{'Procedure':<25} {'Status':<10} {'Start Time':<20} {'Rows':<8} {'Duration':<8}")
        print("-" * 80)
        for proc, status, start, end, rows, duration in result:
            start_str = start.strftime("%Y-%m-%d %H:%M:%S") if start else "N/A"
            rows_str = str(rows) if rows else "0"
            duration_str = f"{duration}s" if duration else "N/A"
            print(f"{proc:<25} {status:<10} {start_str:<20} {rows_str:<8} {duration_str:<8}")
    else:
        print("No execution logs found")

def show_data_counts(cursor):
    """Show data counts across all layers"""
    print("\n📊 Data Counts by Layer:")
    print("=" * 50)
    
    tables = [
        ("Landing Raw", "landing_raw.raw_cdc_places_data", "CDC Places Data"),
        ("Landing Raw", "landing_raw.raw_environmental_health_data", "Environmental Data"),
        ("Curated", "curated.curated_health_indicators", "Health Indicators"),
        ("Curated", "curated.curated_environmental_data", "Environmental Data"),
        ("Data Mart", "data_mart.public_health_dashboard", "Dashboard Records"),
        ("Data Mart", "data_mart.environmental_risk_summary", "Risk Summary"),
        ("Logging", "logging.pipeline_execution_log", "Execution Logs"),
        ("Logging", "logging.data_quality_log", "Quality Logs")
    ]
    
    for layer, table, description in tables:
        result = execute_query(cursor, f"SELECT COUNT(*) FROM {table}")
        if result:
            count = result[0][0]
            print(f"{layer:<12} | {description:<25} | {count:>8} rows")
        else:
            print(f"{layer:<12} | {description:<25} | ERROR")

def show_sample_data(cursor):
    """Show sample data from each layer"""
    print("\n🔍 Sample Data by Layer:")
    print("=" * 60)
    
    # Raw CDC Places Data
    print("\n📍 Raw CDC Places Data (Latest 3 records):")
    result = execute_query(cursor, """
        SELECT county_name, measure, data_value, year 
        FROM landing_raw.raw_cdc_places_data 
        ORDER BY load_timestamp DESC 
        LIMIT 3
    """)
    if result:
        for county, measure, value, year in result:
            print(f"  • {county}: {measure[:40]}... = {value} ({year})")
    
    # Curated Health Indicators
    print("\n📈 Curated Health Indicators (Latest 3 records):")
    result = execute_query(cursor, """
        SELECT county_name, measure_category, measure_value, data_quality_flag 
        FROM curated.curated_health_indicators 
        ORDER BY load_timestamp DESC 
        LIMIT 3
    """)
    if result:
        for county, category, value, quality in result:
            print(f"  • {county}: {category} = {value} (Quality: {quality})")
    
    # Data Mart Dashboard
    print("\n📊 Public Health Dashboard (All records):")
    result = execute_query(cursor, """
        SELECT 
            county_name, 
            total_population, 
            ROUND(air_quality_avg, 1) as avg_air_quality,
            high_risk_facilities_count
        FROM data_mart.public_health_dashboard 
        ORDER BY county_name
    """)
    if result:
        for county, pop, air_quality, facilities in result:
            pop_str = f"{pop:,}" if pop else "N/A"
            air_str = f"{air_quality}" if air_quality else "N/A"
            fac_str = f"{facilities}" if facilities else "0"
            print(f"  • {county}: Pop: {pop_str}, Air Quality: {air_str}, High Risk: {fac_str}")

def test_masking_policies(cursor):
    """Test data masking with different roles"""
    print("\n🎭 Testing Data Masking Policies:")
    print("=" * 50)
    
    # Test with DATA_ENGINEER_ROLE (should see full data)
    print("\n👤 As DATA_ENGINEER_ROLE (full access):")
    try:
        cursor.execute("USE ROLE DATA_ENGINEER_ROLE")
        result = execute_query(cursor, """
            SELECT facility_address, latitude, longitude 
            FROM curated.curated_environmental_data 
            LIMIT 2
        """)
        if result:
            for address, lat, lon in result:
                print(f"  • Address: {address}")
                print(f"    Coordinates: ({lat}, {lon})")
    except Exception as e:
        print(f"  ❌ Error with DATA_ENGINEER_ROLE: {e}")
    
    # Test with DATA_ANALYST_ROLE (should see masked data)
    print("\n👤 As DATA_ANALYST_ROLE (masked access):")
    try:
        cursor.execute("USE ROLE DATA_ANALYST_ROLE")
        result = execute_query(cursor, """
            SELECT facility_address, latitude, longitude 
            FROM curated.curated_environmental_data 
            LIMIT 2
        """)
        if result:
            for address, lat, lon in result:
                print(f"  • Address: {address}")
                print(f"    Coordinates: ({lat}, {lon})")
    except Exception as e:
        print(f"  ❌ Error with DATA_ANALYST_ROLE: {e}")
    
    # Switch back to DATA_ENGINEER_ROLE
    cursor.execute("USE ROLE DATA_ENGINEER_ROLE")

def show_task_status(cursor):
    """Show status of scheduled tasks"""
    print("\n⏰ Scheduled Tasks Status:")
    print("=" * 60)
    
    try:
        result = execute_query(cursor, "SHOW TASKS")
        if result:
            print(f"{'Task Name':<30} {'State':<12} {'Schedule':<20}")
            print("-" * 60)
            for task in result:
                task_name = task[1] if len(task) > 1 else "Unknown"
                task_state = task[6] if len(task) > 6 else "Unknown"
                schedule = task[7] if len(task) > 7 else "Unknown"
                print(f"{task_name:<30} {task_state:<12} {schedule:<20}")
        else:
            print("No tasks found")
    except Exception as e:
        print(f"❌ Error showing tasks: {e}")

def show_data_quality_summary(cursor):
    """Show data quality summary"""
    print("\n🔍 Data Quality Summary:")
    print("=" * 50)
    
    result = execute_query(cursor, """
        SELECT 
            table_name,
            quality_check_name,
            check_result,
            COUNT(*) as check_count
        FROM logging.data_quality_log 
        GROUP BY table_name, quality_check_name, check_result
        ORDER BY table_name, quality_check_name
    """)
    
    if result:
        current_table = None
        for table, check, result_status, count in result:
            if table != current_table:
                print(f"\n📋 {table}:")
                current_table = table
            status_icon = "✅" if result_status == "PASS" else "❌"
            print(f"  {status_icon} {check}: {result_status} ({count} checks)")
    else:
        print("No data quality logs found")

def main():
    """Main exploration function"""
    print("🔍 Exploring Your Public Health Data Pipeline")
    print("=" * 60)
    print("This will show you what your pipeline has created and how it works!\n")
    
    try:
        # Connect as DATA_ENGINEER_ROLE
        conn = connect_to_snowflake('DATA_ENGINEER_ROLE')
        cursor = conn.cursor()
        
        # Make sure we're in the right context
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        cursor.execute("USE WAREHOUSE DEV_WH")
        
        # Show various aspects of the pipeline
        show_pipeline_execution_log(cursor)
        show_data_counts(cursor)
        show_sample_data(cursor)
        show_data_quality_summary(cursor)
        test_masking_policies(cursor)
        show_task_status(cursor)
        
        # Summary
        print("\n" + "=" * 60)
        print("🎉 Pipeline Exploration Complete!")
        print("\n📝 What you've built:")
        print("  ✅ Automated data ingestion (mock CDC & environmental data)")
        print("  ✅ Data transformation pipeline (raw → curated → data mart)")
        print("  ✅ Role-based security with data masking")
        print("  ✅ Comprehensive logging and monitoring")
        print("  ✅ Scheduled tasks for automation")
        print("  ✅ Production-ready stored procedures")
        print("\n🚀 Your pipeline is ready for xFact! This demonstrates:")
        print("  • Snowflake native development (no DBT)")
        print("  • CI/CD best practices")
        print("  • Data security and governance")
        print("  • Automated monitoring and quality checks")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Exploration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()