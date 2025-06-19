#!/usr/bin/env python3
"""
Deploy masking policies to Snowflake with proper dependency handling
"""
import os
import sys
import snowflake.connector
from typing import List, Tuple

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

def execute_with_error_handling(cursor, command: str, description: str = "") -> bool:
    """Execute a command with error handling and logging."""
    try:
        cursor.execute(command)
        print(f"  ✅ {description or command[:60]}")
        return True
    except Exception as e:
        print(f"  ⚠️ {description or command[:60]} - {str(e)[:100]}")
        return False

def get_policy_column_mappings(cursor) -> List[Tuple[str, str, str, str]]:
    """Get all current masking policy applications."""
    try:
        cursor.execute("""
            SELECT 
                table_schema,
                table_name, 
                column_name,
                policy_name
            FROM information_schema.policy_references 
            WHERE policy_kind = 'MASKING_POLICY'
            AND table_catalog = 'PUBLIC_HEALTH_MODERNIZATION_DEMO'
        """)
        return cursor.fetchall()
    except Exception as e:
        print(f"  ⚠️ Could not fetch policy mappings: {e}")
        return []

def unset_all_masking_policies(cursor, mappings: List[Tuple[str, str, str, str]]) -> None:
    """Unset all masking policies from their columns."""
    print("🔓 Unsetting existing masking policies...")
    
    for schema, table, column, policy in mappings:
        command = f"ALTER TABLE {schema}.{table} MODIFY COLUMN {column} UNSET MASKING POLICY"
        execute_with_error_handling(
            cursor, 
            command, 
            f"Unset {policy} from {schema}.{table}.{column}"
        )

def drop_existing_policies(cursor) -> None:
    """Drop existing masking policies."""
    print("🗑️ Dropping existing masking policies...")
    
    policies_to_drop = [
        ('CURATED', 'address_mask'),
        ('CURATED', 'coordinate_mask'),
        ('DATA_MART', 'population_mask')
    ]
    
    for schema, policy in policies_to_drop:
        cursor.execute(f"USE SCHEMA {schema}")
        execute_with_error_handling(
            cursor,
            f"DROP MASKING POLICY IF EXISTS {policy}",
            f"Drop {policy} from {schema}"
        )

def create_masking_policies(cursor) -> None:
    """Create new masking policies."""
    print("🏗️ Creating new masking policies...")
    
    # Address masking policy
    cursor.execute("USE SCHEMA CURATED")
    address_policy = """
        CREATE MASKING POLICY address_mask AS (val STRING) RETURNS STRING ->
          CASE 
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE') THEN val
            WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN 
              CONCAT(LEFT(val, 10), '*** [MASKED] ***')
            ELSE '[REDACTED]'
          END
    """
    execute_with_error_handling(cursor, address_policy, "Create address_mask policy")
    
    # Coordinate masking policy
    coordinate_policy = """
        CREATE MASKING POLICY coordinate_mask AS (val FLOAT) RETURNS FLOAT ->
          CASE 
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE') THEN val
            WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN ROUND(val, 2)
            ELSE NULL
          END
    """
    execute_with_error_handling(cursor, coordinate_policy, "Create coordinate_mask policy")
    
    # Population masking policy
    cursor.execute("USE SCHEMA DATA_MART")
    population_policy = """
        CREATE MASKING POLICY population_mask AS (val INTEGER) RETURNS INTEGER ->
          CASE 
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_ANALYST_ROLE') THEN val
            ELSE ROUND(val, -3)
          END
    """
    execute_with_error_handling(cursor, population_policy, "Create population_mask policy")

def apply_masking_policies(cursor) -> None:
    """Apply masking policies to appropriate columns."""
    print("🔒 Applying masking policies to columns...")
    
    policy_applications = [
        ('CURATED', 'curated_environmental_data', 'facility_address', 'address_mask'),
        ('CURATED', 'curated_health_indicators', 'latitude', 'coordinate_mask'),
        ('CURATED', 'curated_health_indicators', 'longitude', 'coordinate_mask'),
        ('DATA_MART', 'public_health_dashboard', 'total_population', 'population_mask')
    ]
    
    for schema, table, column, policy in policy_applications:
        cursor.execute(f"USE SCHEMA {schema}")
        command = f"ALTER TABLE {table} MODIFY COLUMN {column} SET MASKING POLICY {policy}"
        execute_with_error_handling(
            cursor,
            command,
            f"Apply {policy} to {schema}.{table}.{column}"
        )

def verify_deployment(cursor) -> None:
    """Verify that policies are properly deployed."""
    print("🔍 Verifying masking policy deployment...")
    
    try:
        # Check policies in CURATED schema
        cursor.execute("USE SCHEMA CURATED")
        cursor.execute("SHOW MASKING POLICIES")
        curated_policies = cursor.fetchall()
        print(f"  ✅ CURATED schema has {len(curated_policies)} masking policies")
        
        # Check policies in DATA_MART schema
        cursor.execute("USE SCHEMA DATA_MART")
        cursor.execute("SHOW MASKING POLICIES")
        datamart_policies = cursor.fetchall()
        print(f"  ✅ DATA_MART schema has {len(datamart_policies)} masking policies")
        
        # Check policy applications
        mappings = get_policy_column_mappings(cursor)
        print(f"  ✅ {len(mappings)} masking policies are applied to columns")
        
    except Exception as e:
        print(f"  ⚠️ Verification failed: {e}")

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying masking policies to {environment} environment")
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        # Get current policy mappings before making changes
        current_mappings = get_policy_column_mappings(cursor)
        print(f"📋 Found {len(current_mappings)} existing policy applications")
        
        # Step 1: Unset all existing masking policies
        if current_mappings:
            unset_all_masking_policies(cursor, current_mappings)
        
        # Step 2: Drop existing policies
        drop_existing_policies(cursor)
        
        # Step 3: Create new policies
        create_masking_policies(cursor)
        
        # Step 4: Apply policies to columns
        apply_masking_policies(cursor)
        
        # Step 5: Verify deployment
        verify_deployment(cursor)
        
        print("🎉 Masking policy deployment completed successfully!")
        
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()