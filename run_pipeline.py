"""
Reddit Lakehouse Data Pipeline: Master CLI Entry Point.
Executes the Medallion architecture end-to-end:
Bronze Ingestion -> Silver Cleaning & Parquet -> Gold Star Schema (DuckDB) -> Visualizations.
"""

import argparse
import sys
import time
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extractor import RedditExtractor
from src.transformer import RedditTransformer
from src.warehouse import RedditDataWarehouse
from src.visualizations import PipelineVisualizer


def run_full_pipeline(posts_per_sub: int = 1000, days_window: int = 60):
    """
    Executes the complete Medallion Lakehouse ELT pipeline and generates analytics.
    """
    print("\n" + "=" * 80)
    print("🚀 STARTING REDDIT LAKEHOUSE DATA PIPELINE (END-TO-END EXECUTION)")
    print("=" * 80)
    overall_start = time.time()

    # Step 1: Bronze Layer Ingestion
    print("\n[STEP 1/4] 🥉 INGESTING BRONZE LAYER (RAW PARTITIONED JSON)")
    extractor = RedditExtractor()
    t0 = time.time()
    ext_result = extractor.run_extraction(posts_per_subreddit=posts_per_sub, days_window=days_window)
    print(f"  ✓ Ingested {ext_result['total_records']:,} submissions across {ext_result['files_written']} partitions")
    print(f"  ✓ Subreddits: {', '.join(ext_result['subreddits'])}")
    print(f"  ✓ Ingestion Latency: {time.time() - t0:.2f}s")

    # Step 2: Silver Layer Transformation
    print("\n[STEP 2/4] 🥈 TRANSFORMING SILVER LAYER (CLEANING, DEDUP & PARQUET)")
    transformer = RedditTransformer()
    t0 = time.time()
    trans_result = transformer.run_transformation()
    dq = trans_result["data_quality_report"]
    print(f"  ✓ Loaded {trans_result['load_stats']['raw_record_count']:,} raw records")
    print(f"  ✓ Deduplicated to {trans_result['record_count']:,} unique records ({dq['duplicates_removed']} duplicates purged)")
    print(f"  ✓ Schema Validation: {'PASSED' if dq['schema_valid'] else 'FAILED'}")
    print(f"  ✓ Data Quality Health: {dq['health_status']} (Max Null Rate: {dq['max_column_null_rate'] * 100:.2f}%)")
    print(f"  ✓ Persisted to: {trans_result['silver_output_file']}")
    print(f"  ✓ Transformation Latency: {time.time() - t0:.2f}s")

    # Step 3: Gold Layer Dimensional Modeling
    print("\n[STEP 3/4] 🥇 BUILDING GOLD LAYER (KIMBALL STAR SCHEMA IN DUCKDB)")
    warehouse = RedditDataWarehouse()
    t0 = time.time()
    wh_result = warehouse.build_star_schema()
    counts = wh_result["table_counts"]
    print(f"  ✓ dim_subreddit:          {counts['dim_subreddit_count']:,} records")
    print(f"  ✓ dim_author:             {counts['dim_author_count']:,} records")
    print(f"  ✓ dim_time:               {counts['dim_time_count']:,} records")
    print(f"  ✓ fact_post_engagement:   {counts['fact_post_engagement_count']:,} records")
    print(f"  ✓ Exported 7 Parquet tables to data/lakehouse/gold/")
    print(f"  ✓ OLAP Database: {wh_result['database_path']}")
    print(f"  ✓ Warehouse Modeling Latency: {time.time() - t0:.2f}s")

    # Step 4: Analytical Visualizations
    print("\n[STEP 4/4] 📊 GENERATING PUBLICATION-GRADE VISUALIZATIONS")
    visualizer = PipelineVisualizer(warehouse)
    t0 = time.time()
    viz_result = visualizer.generate_all_visualizations(dq)
    for name, path in viz_result.items():
        print(f"  ✓ Saved artifact: {Path(path).name}")
    print(f"  ✓ Visualization Latency: {time.time() - t0:.2f}s")

    # Pipeline Summary
    total_elapsed = time.time() - overall_start
    print("\n" + "=" * 80)
    print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY IN {total_elapsed:.2f} SECONDS!")
    print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Reddit Lakehouse ELT & Analytics Runner")
    parser.add_argument("--posts-per-sub", type=int, default=1000, help="Number of posts per subreddit (default: 1000)")
    parser.add_argument("--days-window", type=int, default=60, help="Ingestion window in days (default: 60)")
    args = parser.parse_args()

    run_full_pipeline(posts_per_sub=args.posts_per_sub, days_window=args.days_window)


if __name__ == "__main__":
    main()
