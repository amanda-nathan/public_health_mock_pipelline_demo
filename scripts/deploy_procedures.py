#!/usr/bin/env python3
"""
Deploy stored procedures to Snowflake
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
        role=os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),  # ✅ Fixed
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),  # ✅ Fixed
        database=os.environ.get('SNOWFLAKE_DATABASE', 'PUBLIC_HEALTH_MODERNIZATION_DEMO')
    )

def execute_sql_file(cursor, file_path):
    """Execute SQL file with multiple statements"""
    print(f"📄 Deploying procedure from {file_path}")
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Split the file into individual statements
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    
    success_count = 0
    for i, statement in enumerate(statements):
        try:
            cursor.execute(statement)
            success_count += 1
            print(f"  ✅ Statement {i+1}/{len(statements)} executed")
        except Exception as e:
            print(f"  ⚠️ Statement {i+1}/{len(statements)} failed: {str(e)[:150]}")
    
    print(f"📄 {file_path}: {success_count}/{len(statements)} statements succeeded")
    return success_count > 0

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying stored procedures to {environment} environment")
    
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    # Get procedure files
    proc_files = [
        'sql/procs/sp_ingest_raw_data.sql',
        'sql/procs/sp_process_curated_data.sql',
        'sql/procs/sp_build_datamart.sql'
    ]
    
    try:
        executed_count = 0
        for proc_file in proc_files:
            if Path(proc_file).exists():
                if execute_sql_file(cursor, proc_file):
                    executed_count += 1
            else:
                print(f"⚠️ File not found: {proc_file}")
        
        print(f"🎉 Stored procedures deployment completed! {executed_count}/{len(proc_files)} files executed successfully")
        
    except Exception as e:
        print(f"❌ Stored procedures deployment failed: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()