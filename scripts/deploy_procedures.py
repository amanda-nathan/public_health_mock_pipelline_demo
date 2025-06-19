#!/usr/bin/env python3
"""
Deploy stored procedures to Snowflake
"""
import os
import sys
import snowflake.connector
from pathlib import Path

def connect_to_snowflake():
   
    return snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        role=os.environ.get('SNOWFLAKE_ROLE', 'DATA_ENGINEER_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'DEV_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'PUBLIC_HEALTH_MODERNIZATION_DEMO')
    )

def execute_sql_file(cursor, file_path):
    print(f"Deploying procedure from {file_path}")
    with open(file_path, 'r') as file:
        sql_content = file.read()
        
    try:
        cursor.execute(sql_content)
        print(f" Procedure deployed successfully")
    except Exception as e:
        print(f"  Error deploying procedure: {e}")
        raise

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"Deploying stored procedures to {environment} environment")
    
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    # Get procedure files
    proc_files = [
        'sql/procs/sp_ingest_raw_data.sql',
        'sql/procs/sp_process_curated_data.sql',
        'sql/procs/sp_build_datamart.sql'
    ]
    
    try:
        for proc_file in proc_files:
            if Path(proc_file).exists():
                execute_sql_file(cursor, proc_file)
            else:
                print(f" File not found: {proc_file}")
        
        print(" Stored procedures deployment completed successfully")
        
    except Exception as e:
        print(f" Stored procedures deployment failed: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()