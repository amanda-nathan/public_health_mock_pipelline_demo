# Public Health Modernization Mock Data Pipeline

This project demonstrates an automated data pipeline built entirely with native Snowflake features and CI/CD principles using GitHub Actions. It is intended as a mock public health data platform and highlights best practices for modern data engineering, automation, and governance in Snowflake.

> **Note**: This project was developed using a free Snowflake trial account to simulate a real-world environment.

## Project Goals
- Showcase Snowflake native development for structured, secure, and testable public health pipelines.
- Simulate a production-ready ingestion and transformation pipeline for multiple data sources.
- Practice CI/CD workflows to manage SQL and Python-based deployments.
- Demonstrate masking policies and role-based data access in a health data context.

## Architecture Overview
- **Database**: `PUBLIC_HEALTH_MODERNIZATION_DEMO`
- **Schemas**: `LANDING_RAW`, `CURATED`, `DATA_MART`, `LOGGING`
- **Warehouse(s)**: `DEV_WH`, `PROD_WH`
- **Roles**: `DATA_ENGINEER_ROLE`, `DATA_ANALYST_ROLE`, `PUBLIC_HEALTH_ROLE`
- **Security**: Column-level masking policies for PII and sensitive values.
- **Automation**: Scheduled Snowflake tasks, logging, and CI/CD workflows via GitHub Actions.

## Pipeline Flow
1. **Ingestion (Landing Layer)**
   - Raw mock data is inserted into `LANDING_RAW` tables via `sp_ingest_raw_data()`.
2. **Curation (Curated Layer)**
   - Cleaned and standardized data moved to curated tables using `sp_process_curated_data()`.
   - Quality checks are logged in `LOGGING.DATA_QUALITY_LOG`.
3. **Aggregation (Data Mart Layer)**
   - Aggregated insights are written to `DATA_MART` using `sp_build_datamart()`.
4. **Logging**
   - Execution logs and data quality metrics are captured in dedicated logging tables.

## Key Features
### Role-Based Access Control
- Fine-grained role assignments and masking policies (e.g., `address_mask`, `coordinate_mask`, `population_mask`).

### Stored Procedures
- Modular procedures for ingestion, processing, and aggregation.
- Built-in error logging and success metrics.

### Automated Tests and Monitoring
- GitHub Action validates deployments and tests pipeline operations.
- `scripts/explore_pipeline.py` showcases logs, row counts, masking visibility, and task status.
- **Note**: Some tests may appear to pass due to error handling design (e.g., task creation attempts that fail quietly). Test output should be reviewed manually in GitHub Actions logs to verify expected behavior.

### CI/CD with GitHub Actions
- `.github/workflows/deploy_to_snowflake.yml` manages:
  - SQL structure validation
  - Role and database setup
  - Masking policy deployment
  - Task and procedure registration
  - Full pipeline execution

You can inspect the CI/CD results directly in [GitHub Actions](https://github.com/amanda-nathan/public_health_mock_pipelline_demo/actions).

## Directory Structure
```
sql/
  ddl/
    01_setup_roles_and_db.sql
    02_create_tables_and_stages.sql
    03_create_masking_policies.sql
    04_create_logging.sql
  tasks/
    01_create_tasks.sql
scripts/
  deploy_ddl.py
  deploy_procedures.py
  deploy_masking_policies.py
  deploy_tasks.py
  run_tests.py
  explore_pipeline.py
github/
  workflows/
    deploy_to_snowflake.yml
```

## How to Run Locally
Ensure you have the following environment variables set:
```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE=DATA_ENGINEER_ROLE
export SNOWFLAKE_WAREHOUSE=DEV_WH
```
Then you can execute pipeline steps manually:
```bash
python scripts/run_tests.py bootstrap
python scripts/explore_pipeline.py
```

## Potential Improvements
- Parameterize ingestion for external file formats and simulate real S3 stages.
- Add Slack/email alerts for failed executions.
- Modularize logging into a shared Snowflake function.
- Enhance data validation rules (e.g., column-level assertions).
- Add promotion logic between dev → prod schemas.
- Introduce approval workflows before production deployment.

## Summary
This project simulates a secure, scalable, and well-monitored data pipeline for public health data using only Snowflake and GitHub Actions. It illustrates core CI/CD concepts such as modular deployments, stored procedure automation, data quality checks, and role-based access enforcement.

While orchestration tools like Control-M aren't used here, this demonstrates the principles of dependency management and task automation that would integrate easily into enterprise systems.

## Visual Overview (Barebones CI/CD Architecture)
```
GitHub → CI/CD Pipeline → Snowflake Structure
   │         │
   │         ├── deploy_to_snowflake.yml
   │         └── run_tests.py / explore_pipeline.py
   │
   └── Triggers code deploy:
         ├── Ingestion Procedure → LANDING_RAW
         ├── Transformation Procedure → CURATED
         ├── Aggregation Procedure → DATA_MART
         └── Logging / Masking / Task Scheduling
```

Refer to the GitHub Actions tab to confirm specific job output and verify test status in practice.
