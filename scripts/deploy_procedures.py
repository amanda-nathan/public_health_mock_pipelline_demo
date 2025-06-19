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
    """Execute SQL file containing stored procedure"""
    print(f"📄 Deploying procedure from {file_path}")
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Check if this is a stored procedure file
    if 'CREATE OR REPLACE PROCEDURE' in sql_content:
        # For stored procedures, we need to handle the $$ blocks specially
        # Split only on semicolons that are NOT inside $$ blocks
        
        # Simple approach: execute setup statements first, then the procedure
        lines = sql_content.strip().split('\n')
        setup_statements = []
        procedure_start = -1
        
        for i, line in enumerate(lines):
            if 'CREATE OR REPLACE PROCEDURE' in line:
                procedure_start = i
                break
            elif line.strip() and not line.strip().startswith('--'):
                setup_statements.append(line.strip())
        
        # Execute setup statements (USE DATABASE, USE SCHEMA, etc.)
        setup_sql = '\n'.join(setup_statements)
        if setup_sql:
            for stmt in setup_sql.split(';'):
                stmt = stmt.strip()
                if stmt:
                    try:
                        cursor.execute(stmt)
                        print(f"  ✅ Setup statement executed")
                    except Exception as e:
                        print(f"  ⚠️ Setup failed: {str(e)[:100]}")
        
        # Execute the procedure as one block
        if procedure_start >= 0:
            procedure_sql = '\n'.join(lines[procedure_start:])
            try:
                cursor.execute(procedure_sql)
                print(f"  ✅ Stored procedure deployed successfully")
                return True
            except Exception as e:
                print(f"  ❌ Procedure deployment failed: {str(e)[:200]}")
                return False
    else:
        # For non-procedure files, split on semicolons
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        success_count = 0
        for i, statement in enumerate(statements):
            try:
                cursor.execute(statement)
                success_count += 1
                print(f"  ✅ Statement {i+1}/{len(statements)} executed")
            except Exception as e:
                print(f"  ⚠️ Statement {i+1}/{len(statements)} failed: {str(e)[:150]}")
        
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