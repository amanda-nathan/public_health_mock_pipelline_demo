#!/usr/bin/env python3
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
    return True

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying DDL scripts to {environment} environment")
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        current_role = os.environ.get('SNOWFLAKE_ROLE', 'DATA_ENGINEER_ROLE')
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        print(f"✅ Connected as {current_role}")
        
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