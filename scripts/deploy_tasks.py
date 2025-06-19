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
    print(f"📄 Deploying tasks from {file_path}")
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
    
    success_count = 0
    for i, command in enumerate(commands):
        try:
            cursor.execute(command)
            success_count += 1
            if 'CREATE' in command.upper() and 'TASK' in command.upper():
                task_name = command.split()[3] if len(command.split()) > 3 else "Unknown"
                print(f"  ✅ Task created: {task_name}")
            else:
                print(f"  ✅ Command {i+1}/{len(commands)}")
        except Exception as e:
            print(f"  ⚠️ Command {i+1}/{len(commands)} failed: {str(e)[:150]}")
    
    print(f"📄 {file_path}: {success_count}/{len(commands)} commands succeeded")
    return True

def resume_tasks(cursor):
    print("🔄 Resuming tasks...")
    try:
        cursor.execute("SHOW TASKS")
        tasks = cursor.fetchall()
        
        resumed_count = 0
        for task in tasks:
            task_name = task[1] if len(task) > 1 else None
            if task_name:
                try:
                    cursor.execute(f"ALTER TASK {task_name} RESUME")
                    print(f"  ✅ Resumed task: {task_name}")
                    resumed_count += 1
                except Exception as e:
                    print(f"  ⚠️ Failed to resume {task_name}: {str(e)[:100]}")
        
        print(f"🔄 Resumed {resumed_count} tasks")
        return True
    except Exception as e:
        print(f"⚠️ Error resuming tasks: {e}")
        return False

def show_task_status(cursor):
    print("📋 Current task status:")
    try:
        cursor.execute("SHOW TASKS")
        tasks = cursor.fetchall()
        
        if tasks:
            print(f"{'Task Name':<30} {'State':<12} {'Schedule':<20}")
            print("-" * 65)
            for task in tasks:
                task_name = task[1] if len(task) > 1 else "Unknown"
                task_state = task[6] if len(task) > 6 else "Unknown"
                schedule = task[7] if len(task) > 7 else "Unknown"
                print(f"{task_name:<30} {task_state:<12} {schedule:<20}")
        else:
            print("No tasks found")
    except Exception as e:
        print(f"❌ Error showing tasks: {e}")

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying tasks to {environment} environment")
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        current_role = os.environ.get('SNOWFLAKE_ROLE', 'DATA_ENGINEER_ROLE')
        current_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE', 'DEV_WH')
        
        cursor.execute("USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO")
        print(f"✅ Connected as {current_role} using {current_warehouse}")
        
        task_files = [
            'sql/tasks/01_create_tasks.sql'
        ]
        
        executed_count = 0
        for task_file in task_files:
            if Path(task_file).exists():
                if execute_sql_file(cursor, task_file):
                    executed_count += 1
            else:
                print(f"⚠️ File not found: {task_file}")
        
        if executed_count > 0:
            resume_tasks(cursor)
        
        show_task_status(cursor)
        
        print(f"🎉 Task deployment completed! {executed_count}/{len(task_files)} files executed successfully")
        
    except Exception as e:
        print(f"❌ Task deployment failed: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()