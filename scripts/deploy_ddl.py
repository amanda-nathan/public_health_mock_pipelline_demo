#!/usr/bin/env python3
"""
Deploy DDL scripts to Snowflake
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
  
    print(f"Executing {file_path}")
    with open(file_path, 'r') as file:
        sql_commands = file.read()
        
   
    for command in sql_commands.split(';'):
        command = command.strip()
        if command:
            try:
                cursor.execute(command)
                print(f"Executed command successfully")
            except Exception as e:
                print(f"Error executing command: {e}")
                raise

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"Deploying DDL scripts to {environment} environment")
    
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    # Get DDL files in order
    ddl_files = [
        'sql/ddl/01_setup_roles_and_db.sql',
        'sql/ddl/02_create_tables_and_stages.sql',
        'sql/ddl/03_create_masking_policies.sql',
        'sql/ddl/04_create_logging.sql'
    ]
    
    try:
        for ddl_file in ddl_files:
            if Path(ddl_file).exists():
                execute_sql_file(cursor, ddl_file)
            else:
                print(f"  File not found: {ddl_file}")
        
        print("DDL deployment completed successfully")
        
    except Exception as e:
        print(f"DDL deployment failed: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()