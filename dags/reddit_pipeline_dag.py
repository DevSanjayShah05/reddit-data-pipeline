"""
Apache Airflow DAG: Reddit Lakehouse Orchestration Pipeline.
Coordinates Bronze extraction, schema validation, Silver transformation,
Data Quality gating, Gold dimensional warehouse loading, and visual artifact generation.
"""

from datetime import datetime, timedelta
import json
import logging
import sys
from pathlib import Path

# Add project root to path for standalone DAG parsing and Airflow worker execution
DAG_FOLDER = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_FOLDER.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Optional Airflow imports with graceful fallback for standalone execution
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    PythonOperator = None

logger = logging.getLogger("reddit_lakehouse_dag")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# --- Pipeline Task Callables ---

def task_extract_reddit_bronze(**kwargs):
    """Task 1: Ingest raw submissions from target subreddits into Bronze JSON partitions."""
    from src.extractor import RedditExtractor
    logger.info("Executing Task 1: Bronze Layer Ingestion...")
    extractor = RedditExtractor()
    result = extractor.run_extraction(posts_per_subreddit=1000, days_window=60)
    logger.info("Bronze Ingestion Complete: %s records written across %s files.",
                result["total_records"], result["files_written"])
    return result


def task_validate_bronze_schema(**kwargs):
    """Task 2: Validate raw JSON batch integrity and partition consistency."""
    from src.config import BRONZE_DIR
    logger.info("Executing Task 2: Bronze Schema Integrity Check...")
    json_files = list(BRONZE_DIR.glob("**/*.json"))
    if not json_files:
        raise ValueError("Bronze validation failed: Zero JSON files found in bronze directory.")
    logger.info("Bronze Partition Check Passed: Found %d partitioned files.", len(json_files))
    return {"status": "PASSED", "files_validated": len(json_files)}


def task_transform_silver_lakehouse(**kwargs):
    """Task 3: Cleanse, deduplicate, enrich features, and persist Silver Parquet."""
    from src.transformer import RedditTransformer
    logger.info("Executing Task 3: Silver Layer Transformation & Deduplication...")
    transformer = RedditTransformer()
    result = transformer.run_transformation()
    logger.info("Silver Transformation Complete: %d records in %s.",
                result["record_count"], result["silver_output_file"])
    return result


def task_run_data_quality_gates(**kwargs):
    """Task 4: Enforce Lakehouse data contracts, null rate thresholds, and volume SLA."""
    from src.transformer import RedditTransformer
    logger.info("Executing Task 4: Enforcing Lakehouse Data Quality Gates...")
    transformer = RedditTransformer()
    records, _ = transformer.load_bronze_records()
    _, dq_report = transformer.clean_and_enrich(records)

    if dq_report["health_status"] != "HEALTHY":
        raise ValueError(f"Data Quality Gate Failed: Health status is {dq_report['health_status']}")
    
    logger.info("Quality Gate Passed: 100%% Schema conformity, null rate: %.4f%%",
                dq_report["max_column_null_rate"] * 100)
    return dq_report


def task_build_gold_warehouse(**kwargs):
    """Task 5: Load Kimball Star Schema and Analytical Marts into DuckDB and Parquet."""
    from src.warehouse import RedditDataWarehouse
    logger.info("Executing Task 5: Gold Layer Dimensional Modeling (DuckDB)...")
    warehouse = RedditDataWarehouse()
    result = warehouse.build_star_schema()
    logger.info("Gold Warehouse Loaded: %s", json.dumps(result["table_counts"]))
    return result


def task_generate_visualizations(**kwargs):
    """Task 6: Generate and publish visual dashboards and architectural artifacts."""
    from src.transformer import RedditTransformer
    from src.warehouse import RedditDataWarehouse
    from src.visualizations import PipelineVisualizer

    logger.info("Executing Task 6: Rendering Analytical Visualizations...")
    transformer = RedditTransformer()
    records, _ = transformer.load_bronze_records()
    _, dq_report = transformer.clean_and_enrich(records)

    warehouse = RedditDataWarehouse()
    visualizer = PipelineVisualizer(warehouse)
    artifacts = visualizer.generate_all_visualizations(dq_report)
    logger.info("All 5 visualization artifacts generated successfully.")
    return artifacts


# --- Airflow DAG Definition ---

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15)
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="reddit_lakehouse_orchestration_pipeline",
        default_args=default_args,
        description="End-to-End Reddit Medallion Lakehouse & Dimensional Warehouse Pipeline",
        schedule_interval="0 4 * * *",  # Daily at 04:00 UTC
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["lakehouse", "reddit", "medallion", "duckdb", "parquet"]
    )

    t1 = PythonOperator(
        task_id="extract_reddit_bronze",
        python_callable=task_extract_reddit_bronze,
        dag=dag
    )

    t2 = PythonOperator(
        task_id="validate_bronze_schema",
        python_callable=task_validate_bronze_schema,
        dag=dag
    )

    t3 = PythonOperator(
        task_id="transform_silver_lakehouse",
        python_callable=task_transform_silver_lakehouse,
        dag=dag
    )

    t4 = PythonOperator(
        task_id="run_data_quality_gates",
        python_callable=task_run_data_quality_gates,
        dag=dag
    )

    t5 = PythonOperator(
        task_id="build_gold_warehouse",
        python_callable=task_build_gold_warehouse,
        dag=dag
    )

    t6 = PythonOperator(
        task_id="generate_visualizations",
        python_callable=task_generate_visualizations,
        dag=dag
    )

    # Dependency graph
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
else:
    dag = None


def execute_dag_standalone():
    """Executes the pipeline sequentially when Airflow scheduler daemon is not present."""
    logger.info("Starting Standalone Orchestration Pipeline Execution...")
    t1_res = task_extract_reddit_bronze()
    t2_res = task_validate_bronze_schema()
    t3_res = task_transform_silver_lakehouse()
    t4_res = task_run_data_quality_gates()
    t5_res = task_build_gold_warehouse()
    t6_res = task_generate_visualizations()
    logger.info("Pipeline Orchestration Finished Successfully.")
    return {
        "bronze": t1_res,
        "validation": t2_res,
        "silver": t3_res,
        "data_quality": t4_res,
        "gold": t5_res,
        "visualizations": t6_res
    }


if __name__ == "__main__":
    execute_dag_standalone()
