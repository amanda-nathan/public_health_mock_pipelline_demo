#!/usr/bin/env python3
"""
Deploy DDL scripts to Snowflake
"""
import os
import sys
import snowflake.connector
from pathlib import Path

def connect_to_snowflake():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        role=os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'PUBLIC_HEALTH_MODERNIZATION_DEMO')
    )

def execute_sql_file(cursor, file_path):
    """Execute SQL file"""
    print(f"📄 Executing {file_path}")
    with open(file_path, 'r') as file:
        sql_commands = file.read()
        
    commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]
    
    success_count = 0
    for i, command in enumerate(commands):
        try:
            cursor.execute(command)
            success_count += 1
            print(f"  ✅ Command {i+1}/{len(commands)}")
        except Exception as e:
            print(f"  ⚠️ Command {i+1}/{len(commands)} failed: {str(e)[:100]}")
    
    print(f"📄 {file_path}: {success_count}/{len(commands)} commands succeeded")
    return success_count > 0

def grant_comprehensive_permissions(cursor):
    """Grant comprehensive permissions to DATA_ENGINEER_ROLE"""
    print("🔐 Granting comprehensive permissions to DATA_ENGINEER_ROLE...")
    
    grant_commands = [
        # Ensure DATA_ENGINEER_ROLE has full access to all schemas
        "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
        
        # Grant access to all current tables
        "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
        
        # Grant access to all current procedures  
        "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
        "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
        "GRANT USAGE ON ALL PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
        
        # Grant access to all current views
        "GRANT ALL ON ALL VIEWS IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
        
        # Grant access to future objects
        "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
        "GRANT ALL ON FUTURE TABLES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE",
        
        "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING_RAW TO ROLE DATA_ENGINEER_ROLE",
        "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.CURATED TO ROLE DATA_ENGINEER_ROLE",
        "GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.DATA_MART TO ROLE DATA_ENGINEER_ROLE",
        
        "GRANT ALL ON FUTURE VIEWS IN SCHEMA PUBLIC_HEALTH_MODERNIZATION_DEMO.LOGGING TO ROLE DATA_ENGINEER_ROLE"
    ]
    
    success_count = 0
    for command in grant_commands:
        try:
            cursor.execute(command)
            success_count += 1
            print(f"  ✅ {command[:80]}...")
        except Exception as e:
            print(f"  ⚠️ Grant failed: {command[:60]}... - {str(e)[:100]}")
    
    print(f"🔐 Applied {success_count}/{len(grant_commands)} permission grants")
    return True

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying DDL scripts to {environment} environment")
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        # Ensure we're using ACCOUNTADMIN for deployment
        cursor.execute("USE ROLE ACCOUNTADMIN")
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        
        ddl_files = [
            'sql/ddl/02_create_tables_and_stages.sql',
            'sql/ddl/04_create_logging.sql'
        ]
        
        executed_count = 0
        for ddl_file in ddl_files:
            if Path(ddl_file).exists():
                if execute_sql_file(cursor, ddl_file):
                    executed_count += 1
            else:
                print(f"⚠️ File not found: {ddl_file}")
        
        # Apply comprehensive permissions
        grant_comprehensive_permissions(cursor)
        
        print(f"🎉 DDL deployment completed successfully! {executed_count}/{len(ddl_files)} files executed")
        
    except Exception as e:
        print(f"❌ DDL deployment failed: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()