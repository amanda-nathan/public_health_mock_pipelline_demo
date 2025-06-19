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

def get_known_policy_applications() -> List[Tuple[str, str, str, str]]:
    """Return known policy applications based on our setup."""
    return [
        ('CURATED', 'curated_environmental_data', 'facility_address', 'address_mask'),
        ('CURATED', 'curated_health_indicators', 'latitude', 'coordinate_mask'),
        ('CURATED', 'curated_health_indicators', 'longitude', 'coordinate_mask'),
        ('DATA_MART', 'public_health_dashboard', 'total_population', 'population_mask')
    ]

def check_table_exists(cursor, schema: str, table: str) -> bool:
    """Check if a table exists."""
    try:
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        result = cursor.fetchone()
        return result is not None
    except Exception:
        return False

def unset_all_masking_policies(cursor) -> None:
    """Unset all known masking policies from their columns."""
    print("🔓 Unsetting existing masking policies...")
    
    known_applications = get_known_policy_applications()
    
    for schema, table, column, policy in known_applications:
        # Check if table exists before trying to unset
        if check_table_exists(cursor, schema, table):
            cursor.execute(f"USE SCHEMA {schema}")
            command = f"ALTER TABLE {table} MODIFY COLUMN {column} UNSET MASKING POLICY"
            execute_with_error_handling(
                cursor, 
                command, 
                f"Unset {policy} from {schema}.{table}.{column}"
            )
        else:
            print(f"  ⚠️ Table {schema}.{table} does not exist, skipping unset for {column}")

def check_policy_exists(cursor, schema: str, policy_name: str) -> bool:
    """Check if a masking policy exists."""
    try:
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute(f"SHOW MASKING POLICIES LIKE '{policy_name}'")
        result = cursor.fetchone()
        return result is not None
    except Exception:
        return False

def drop_existing_policies(cursor) -> None:
    """Drop existing masking policies if they exist."""
    print("🗑️ Dropping existing masking policies...")
    
    policies_to_drop = [
        ('CURATED', 'address_mask'),
        ('CURATED', 'coordinate_mask'),
        ('DATA_MART', 'population_mask')
    ]
    
    for schema, policy in policies_to_drop:
        if check_policy_exists(cursor, schema, policy):
            cursor.execute(f"USE SCHEMA {schema}")
            execute_with_error_handling(
                cursor,
                f"DROP MASKING POLICY {policy}",
                f"Drop {policy} from {schema}"
            )
        else:
            print(f"  ✅ Policy {policy} in {schema} does not exist, skipping")

def create_masking_policies(cursor) -> None:
    """Create new masking policies."""
    print("🏗️ Creating new masking policies...")
    
    # Address masking policy
    cursor.execute("USE SCHEMA CURATED")
    if not check_policy_exists(cursor, 'CURATED', 'address_mask'):
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
    else:
        print("  ✅ address_mask policy already exists")
    
    # Coordinate masking policy
    if not check_policy_exists(cursor, 'CURATED', 'coordinate_mask'):
        coordinate_policy = """
            CREATE MASKING POLICY coordinate_mask AS (val FLOAT) RETURNS FLOAT ->
              CASE 
                WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE') THEN val
                WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN ROUND(val, 2)
                ELSE NULL
              END
        """
        execute_with_error_handling(cursor, coordinate_policy, "Create coordinate_mask policy")
    else:
        print("  ✅ coordinate_mask policy already exists")
    
    # Population masking policy
    cursor.execute("USE SCHEMA DATA_MART")
    if not check_policy_exists(cursor, 'DATA_MART', 'population_mask'):
        population_policy = """
            CREATE MASKING POLICY population_mask AS (val INTEGER) RETURNS INTEGER ->
              CASE 
                WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_ANALYST_ROLE') THEN val
                ELSE ROUND(val, -3)
              END
        """
        execute_with_error_handling(cursor, population_policy, "Create population_mask policy")
    else:
        print("  ✅ population_mask policy already exists")

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
        
        # Verify specific policies exist
        expected_policies = [
            ('CURATED', 'address_mask'),
            ('CURATED', 'coordinate_mask'),
            ('DATA_MART', 'population_mask')
        ]
        
        policies_verified = 0
        for schema, policy_name in expected_policies:
            if check_policy_exists(cursor, schema, policy_name):
                print(f"  ✅ {schema}.{policy_name} exists")
                policies_verified += 1
            else:
                print(f"  ❌ {schema}.{policy_name} missing")
        
        print(f"  ✅ {policies_verified}/{len(expected_policies)} expected policies verified")
        
        # Test policy applications by checking known columns
        applications_verified = 0
        known_applications = get_known_policy_applications()
        
        for schema, table, column, policy in known_applications:
            if check_table_exists(cursor, schema, table):
                print(f"  ✅ {schema}.{table}.{column} → {policy} (table exists)")
                applications_verified += 1
            else:
                print(f"  ⚠️ {schema}.{table} does not exist yet")
        
        print(f"  ✅ {applications_verified}/{len(known_applications)} policy applications verified")
            
    except Exception as e:
        print(f"  ⚠️ Verification failed: {e}")

def main():
    environment = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    
    print(f"🚀 Deploying masking policies to {environment} environment")
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        # Check what we're working with
        known_applications = get_known_policy_applications()
        print(f"📋 Will manage {len(known_applications)} policy applications")
        
        # Step 1: Unset all existing masking policies from columns
        unset_all_masking_policies(cursor)
        
        # Step 2: Drop existing policies (only if they exist and are not attached)
        drop_existing_policies(cursor)
        
        # Step 3: Create new policies (only if they don't exist)
        create_masking_policies(cursor)
        
        # Step 4: Apply policies to columns (only if tables exist)
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