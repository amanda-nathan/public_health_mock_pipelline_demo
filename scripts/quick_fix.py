#!/usr/bin/env python3
"""
Quick fix script to ensure everything is set up correctly
Run this manually if you need to fix permissions and verify the setup
"""
import os
import snowflake.connector

def main():
    print("🔧 Running quick fix for Public Health Pipeline...")
    
    try:
        # Connect as ACCOUNTADMIN
        conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            role='ACCOUNTADMIN',
            warehouse='COMPUTE_WH',
            database='PUBLIC_HEALTH_MODERNIZATION_DEMO'
        )
        
        cursor = conn.cursor()
        
        print("✅ Connected as ACCOUNTADMIN")
        
        # 1. Verify database structure
        print("\n📋 Checking database structure...")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        schema_names = [s[1] for s in schemas if s[1] in ['LANDING_RAW', 'CURATED', 'DATA_MART', 'LOGGING']]
        print(f"   Found schemas: {', '.join(schema_names)}")
        
        # 2. Check tables exist
        print("\n📊 Checking tables...")
        for schema in schema_names:
            cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
            tables = cursor.fetchall()
            table_names = [t[1] for t in tables]
            print(f"   {schema}: {len(table_names)} tables")
        
        # 3. Grant comprehensive permissions
        print("\n🔐 Applying comprehensive permissions...")
        
        permission_commands = [
            # Core grants
            "GRANT USAGE ON DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
            
            # All current objects
            "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
            
            "GRANT ALL ON ALL VIEWS IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
            
            "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
            "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
            "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
            
            # Future objects
            "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
            "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
            
            "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
            "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
            "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
            
            # Warehouse access
            "GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER_ROLE",
            "GRANT USAGE ON WAREHOUSE DEV_WH TO ROLE DATA_ENGINEER_ROLE"
        ]
        
        success_count = 0
        for cmd in permission_commands:
            try:
                cursor.execute(cmd)
                success_count += 1
            except Exception as e:
                print(f"   ⚠️ {cmd[:60]}... - {str(e)[:80]}")
        
        print(f"   ✅ Applied {success_count}/{len(permission_commands)} permissions")
        
        # 4. Test pipeline with DATA_ENGINEER_ROLE
        print("\n🧪 Testing pipeline as DATA_ENGINEER_ROLE...")
        cursor.execute("USE ROLE DATA_ENGINEER_ROLE")
        cursor.execute("USE WAREHOUSE DEV_WH")
        
        # Test each procedure
        test_procedures = [
            ("LANDING_RAW.SP_INGEST_RAW_DATA('CDC_PLACES')", "CDC Places ingestion"),
            ("LANDING_RAW.SP_INGEST_RAW_DATA('ENVIRONMENTAL')", "Environmental ingestion"),
            ("CURATED.SP_PROCESS_CURATED_DATA()", "Curated processing"),
            ("DATA_MART.SP_BUILD_DATAMART()", "Data mart building")
        ]
        
        pipeline_success = True
        for call, description in test_procedures:
            try:
                cursor.execute(f"CALL {call}")
                result = cursor.fetchone()
                result_msg = result[0] if result else 'No result'
                
                if 'ERROR:' in result_msg:
                    print(f"   ❌ {description}: {result_msg[:100]}")
                    pipeline_success = False
                else:
                    print(f"   ✅ {description}: {result_msg[:100]}")
            except Exception as e:
                print(f"   ❌ {description}: {str(e)[:100]}")
                pipeline_success = False
        
        # 5. Show final data counts
        print("\n📊 Final data counts:")
        cursor.execute("USE ROLE DATA_ENGINEER_ROLE")
        
        data_tables = [
            ("LANDING_RAW.RAW_CDC_PLACES_DATA", "Raw CDC Places"),
            ("LANDING_RAW.RAW_ENVIRONMENTAL_HEALTH_DATA", "Raw Environmental"),
            ("CURATED.CURATED_HEALTH_INDICATORS", "Curated Health"),
            ("CURATED.CURATED_ENVIRONMENTAL_DATA", "Curated Environmental"),
            ("DATA_MART.PUBLIC_HEALTH_DASHBOARD", "Dashboard"),
            ("LOGGING.PIPELINE_EXECUTION_LOG", "Execution Log")
        ]
        
        for table, description in data_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {description}: {count} rows")
            except Exception as e:
                print(f"   {description}: ERROR - {str(e)[:50]}")
        
        cursor.close()
        conn.close()
        
        if pipeline_success:
            print("\n🎉 Quick fix completed successfully! Pipeline is working!")
        else:
            print("\n⚠️ Quick fix completed but pipeline still has issues. Check the errors above.")
            
    except Exception as e:
        print(f"❌ Quick fix failed: {e}")

if __name__ == "__main__":
    main()