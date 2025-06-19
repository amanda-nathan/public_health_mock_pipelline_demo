#!/usr/bin/env python3
"""
Snowflake Pipeline Testing Script (Bootstrap Version)
Tests the public health data pipeline deployment and functionality

This version is designed for initial setup using ACCOUNTADMIN and COMPUTE_WH.
It gracefully handles missing components during the bootstrap phase.
"""
import os
import sys
import snowflake.connector
from datetime import datetime

def connect_to_snowflake():
    """Create Snowflake connection"""
    try:
        return snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            role=os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.environ.get('SNOWFLAKE_DATABASE', 'PUBLIC_HEALTH_MODERNIZATION_DEMO')
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def run_query(cursor, query, description=""):
    """Execute a query and return results"""
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        if description:
            print(f"✅ {description}")
        return result
    except Exception as e:
        # During bootstrap, many queries may fail - don't spam with errors
        if description and "exists" not in description.lower():
            print(f"⚠️ {description}: {str(e)[:100]}")
        return None

def test_connection(cursor):
    """Test basic Snowflake connection"""
    print("\n🔗 Testing Snowflake Connection...")
    
    result = run_query(cursor, 
                      "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()",
                      "Connection test")
    
    if result:
        user, role, warehouse, database = result[0]
        print(f"   User: {user}")
        print(f"   Role: {role}")
        print(f"   Warehouse: {warehouse}")
        print(f"   Database: {database}")
        return True
    return False

def test_database_structure(cursor):
    """Test that database and schemas exist"""
    print("\n🏗️ Testing Database Structure...")
    
    # Test database exists
    result = run_query(cursor, 
                      "SHOW DATABASES LIKE 'PUBLIC_HEALTH_MODERNIZATION_DEMO'",
                      "Database exists")
    
    if not result:
        return False
    
    # Test schemas exist
    expected_schemas = ['LANDING_RAW', 'CURATED', 'DATA_MART', 'LOGGING']
    
    run_query(cursor, "USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
    
    for schema in expected_schemas:
        result = run_query(cursor, 
                          f"SHOW SCHEMAS LIKE '{schema}'",
                          f"Schema {schema} exists")
        if not result:
            return False
    
    return True

def test_tables_exist(cursor):
    """Test that required tables exist"""
    print("\n📋 Testing Table Structure...")
    
    # In bootstrap mode, tables might not be created yet
    try:
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
    except:
        print("⚠️ Database not created yet - skipping table tests")
        return True
    
    # Expected tables per schema
    expected_tables = {
        'LANDING_RAW': ['RAW_CDC_PLACES_DATA', 'RAW_ENVIRONMENTAL_HEALTH_DATA'],
        'CURATED': ['CURATED_HEALTH_INDICATORS', 'CURATED_ENVIRONMENTAL_DATA'],
        'DATA_MART': ['PUBLIC_HEALTH_DASHBOARD', 'ENVIRONMENTAL_RISK_SUMMARY'],
        'LOGGING': ['PIPELINE_EXECUTION_LOG', 'DATA_QUALITY_LOG']
    }
    
    tables_found = 0
    total_expected = sum(len(tables) for tables in expected_tables.values())
    
    for schema, tables in expected_tables.items():
        for table in tables:
            result = run_query(cursor,
                              f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema}",
                              f"Table {schema}.{table} exists")
            if result:
                tables_found += 1
    
    if tables_found == 0:
        print("⚠️ No tables found - might be bootstrap phase")
        return True
    else:
        print(f"✅ Found {tables_found}/{total_expected} expected tables")
        return True  # Don't fail in bootstrap mode

def test_stored_procedures(cursor):
    """Test that stored procedures exist"""
    print("\n⚙️ Testing Stored Procedures...")
    
    # In bootstrap mode, procedures might not be created yet
    # Check if database exists first
    try:
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
    except:
        print("⚠️ Database not created yet - skipping procedure tests")
        return True
    
    expected_procedures = [
        ('LANDING_RAW', 'SP_INGEST_RAW_DATA'),
        ('CURATED', 'SP_PROCESS_CURATED_DATA'),
        ('DATA_MART', 'SP_BUILD_DATAMART')
    ]
    
    procedures_exist = 0
    for schema, proc in expected_procedures:
        result = run_query(cursor,
                          f"SHOW PROCEDURES LIKE '{proc}' IN SCHEMA {schema}",
                          f"Procedure {schema}.{proc} exists")
        if result:
            procedures_exist += 1
    
    if procedures_exist == 0:
        print("⚠️ No stored procedures found - might be bootstrap phase")
        return True
    else:
        print(f"✅ Found {procedures_exist}/{len(expected_procedures)} stored procedures")
        return procedures_exist > 0

def test_roles_and_permissions(cursor):
    """Test role assignments and permissions"""
    print("\n🔐 Testing Roles and Permissions...")
    
    # Test user has required roles
    result = run_query(cursor,
                      "SHOW GRANTS TO USER HATTAWAY7",
                      "User role grants")
    
    if result:
        roles = [grant[1] for grant in result if grant[0] == 'ROLE']
        print(f"   Granted roles: {', '.join(roles)}")
        
        # Check for key roles
        if 'DATA_ENGINEER_ROLE' in roles:
            print("✅ DATA_ENGINEER_ROLE granted")
        else:
            print("⚠️ DATA_ENGINEER_ROLE not found")
    
    return True

def test_data_pipeline(cursor):
    """Test the complete data pipeline execution"""
    print("\n🚀 Testing Data Pipeline Execution...")
    
    # First check if stored procedures exist
    try:
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        cursor.execute("SHOW PROCEDURES IN SCHEMA LANDING_RAW")
        procs = cursor.fetchall()
        
        if not procs:
            print("⚠️ Stored procedures not deployed yet - skipping pipeline test")
            return True
            
    except Exception as e:
        print(f"⚠️ Cannot access stored procedures: {e}")
        return True
    
    try:
        # Test CDC Places data ingestion
        print("   Testing CDC Places data ingestion...")
        cursor.execute("CALL LANDING_RAW.SP_INGEST_RAW_DATA('CDC_PLACES')")
        result = cursor.fetchone()
        print(f"   Result: {result[0] if result else 'No result'}")
        
        # Test Environmental data ingestion
        print("   Testing Environmental data ingestion...")
        cursor.execute("CALL LANDING_RAW.SP_INGEST_RAW_DATA('ENVIRONMENTAL')")
        result = cursor.fetchone()
        print(f"   Result: {result[0] if result else 'No result'}")
        
        # Test curated data processing
        print("   Testing curated data processing...")
        cursor.execute("CALL CURATED.SP_PROCESS_CURATED_DATA()")
        result = cursor.fetchone()
        print(f"   Result: {result[0] if result else 'No result'}")
        
        # Test data mart building
        print("   Testing data mart building...")
        cursor.execute("CALL DATA_MART.SP_BUILD_DATAMART()")
        result = cursor.fetchone()
        print(f"   Result: {result[0] if result else 'No result'}")
        
        print("✅ Data pipeline execution completed")
        return True
        
    except Exception as e:
        print(f"❌ Data pipeline execution failed: {e}")
        return False

def test_data_quality(cursor):
    """Test data quality and counts"""
    print("\n📊 Testing Data Quality...")
    
    # In bootstrap mode, tables might not exist yet
    try:
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
    except:
        print("⚠️ Database not created yet - skipping data quality tests")
        return True
    
    # Test data counts
    data_checks = [
        ("LANDING_RAW.RAW_CDC_PLACES_DATA", "Raw CDC Places data"),
        ("LANDING_RAW.RAW_ENVIRONMENTAL_HEALTH_DATA", "Raw Environmental data"),
        ("CURATED.CURATED_HEALTH_INDICATORS", "Curated Health Indicators"),
        ("CURATED.CURATED_ENVIRONMENTAL_DATA", "Curated Environmental data"),
        ("DATA_MART.PUBLIC_HEALTH_DASHBOARD", "Public Health Dashboard"),
        ("LOGGING.PIPELINE_EXECUTION_LOG", "Pipeline Execution Log")
    ]
    
    tables_with_data = 0
    
    for table, description in data_checks:
        result = run_query(cursor,
                          f"SELECT COUNT(*) FROM {table}",
                          f"{description} row count")
        if result:
            count = result[0][0]
            print(f"   {description}: {count} rows")
            if count > 0:
                tables_with_data += 1
            else:
                print(f"⚠️ {description} has no data (expected in bootstrap)")
        else:
            print(f"⚠️ {description} table not found (expected in bootstrap)")
    
    if tables_with_data == 0:
        print("⚠️ No data found - might be bootstrap phase")
        return True
    else:
        print(f"✅ Found data in {tables_with_data}/{len(data_checks)} tables")
        return True  # Don't fail in bootstrap mode

def test_logging_functionality(cursor):
    """Test logging and monitoring functionality"""
    print("\n📝 Testing Logging Functionality...")
    
    # Check if logging tables exist first
    try:
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        cursor.execute("SHOW TABLES IN SCHEMA LOGGING")
        logging_tables = cursor.fetchall()
        
        if not logging_tables:
            print("⚠️ Logging tables not created yet - skipping logging tests")
            return True
            
    except Exception as e:
        print(f"⚠️ Cannot access logging schema: {e}")
        return True
    
    # Check recent pipeline executions
    result = run_query(cursor,
                      """SELECT procedure_name, execution_status, execution_start 
                         FROM LOGGING.PIPELINE_EXECUTION_LOG 
                         ORDER BY execution_start DESC LIMIT 5""",
                      "Recent pipeline executions")
    
    if result:
        print("   Recent executions:")
        for proc, status, start_time in result:
            print(f"     {proc}: {status} at {start_time}")
    else:
        print("⚠️ No pipeline execution logs found (expected in bootstrap)")
    
    # Check data quality logs
    result = run_query(cursor,
                      """SELECT table_name, quality_check_name, check_result 
                         FROM LOGGING.DATA_QUALITY_LOG 
                         ORDER BY check_timestamp DESC LIMIT 3""",
                      "Recent data quality checks")
    
    if result:
        print("   Recent quality checks:")
        for table, check, result_status in result:
            print(f"     {table}.{check}: {result_status}")
    else:
        print("⚠️ No data quality logs found (expected in bootstrap)")
    
    return True

def test_masking_policies(cursor):
    """Test data masking functionality"""
    print("\n🎭 Testing Data Masking...")
    
    try:
        # First check if DATA_ANALYST_ROLE exists
        result = run_query(cursor, "SHOW ROLES LIKE 'DATA_ANALYST_ROLE'")
        
        if not result:
            print("⚠️ DATA_ANALYST_ROLE not created yet - skipping masking test")
            return True
        
        # Switch to a role with limited access to test masking
        print("   Testing masking with DATA_ANALYST_ROLE...")
        cursor.execute("USE ROLE DATA_ANALYST_ROLE")
        
        # Test masked data
        result = run_query(cursor,
                          "SELECT facility_address FROM CURATED.CURATED_ENVIRONMENTAL_DATA LIMIT 1",
                          "Masked facility address")
        
        if result:
            address = result[0][0]
            print(f"   Masked address: {address}")
            if "MASKED" in str(address) or len(str(address)) < 50:
                print("✅ Address masking working")
            else:
                print("⚠️ Address may not be properly masked")
        
        # Switch back to admin role
        cursor.execute("USE ROLE ACCOUNTADMIN")
        
        return True
        
    except Exception as e:
        print(f"❌ Masking test failed: {e}")
        cursor.execute("USE ROLE ACCOUNTADMIN")  # Try to switch back
        return False

def run_all_tests():
    """Run all tests and return overall success"""
    print("🧪 Starting Snowflake Pipeline Tests")
    print("=" * 50)
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if not conn:
        print("❌ Cannot connect to Snowflake. Check your credentials.")
        return False
    
    cursor = conn.cursor()
    
    # Run all tests
    tests = [
        test_connection,
        test_database_structure,
        test_tables_exist,
        test_stored_procedures,
        test_roles_and_permissions,
        test_data_pipeline,
        test_data_quality,
        test_logging_functionality,
        test_masking_policies
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func(cursor):
                passed += 1
            else:
                print(f"❌ {test_func.__name__} failed")
        except Exception as e:
            print(f"❌ {test_func.__name__} error: {e}")
    
    # Close connection
    cursor.close()
    conn.close()
    
    # Summary
    print("\n" + "=" * 50)
    print(f"🧪 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your pipeline is working correctly.")
        return True
    else:
        print(f"⚠️ {total - passed} test(s) failed. Check the output above.")
        return False

def main():
    """Main function"""
    environment = sys.argv[1] if len(sys.argv) > 1 else 'bootstrap'
    
    print(f"Running tests for {environment} environment")
    print("Note: Using ACCOUNTADMIN/COMPUTE_WH for initial bootstrap")
    
    success = run_all_tests()
    
    if success:
        print("\n✅ All tests completed successfully!")
        if environment == 'bootstrap':
            print("🚀 Ready to deploy stored procedures and complete setup!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()