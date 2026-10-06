"""
Unit and Integration Tests for Reddit Lakehouse Data Engineering Pipeline.
Validates extraction, silver schema transformation, data quality gates,
DuckDB dimensional modeling, and foreign key integrity.
"""

import shutil
import tempfile
from pathlib import Path
import pytest
import pandas as pd

from src.extractor import RedditExtractor
from src.transformer import RedditTransformer
from src.warehouse import RedditDataWarehouse


@pytest.fixture(scope="session")
def test_env():
    """Sets up an isolated temporary directory for pipeline tests."""
    temp_dir = Path(tempfile.mkdtemp(prefix="test_reddit_lakehouse_"))
    bronze_dir = temp_dir / "bronze"
    silver_dir = temp_dir / "silver"
    gold_dir = temp_dir / "gold"
    db_path = gold_dir / "test_warehouse.duckdb"

    yield {
        "root": temp_dir,
        "bronze": bronze_dir,
        "silver": silver_dir,
        "gold": gold_dir,
        "db_path": db_path
    }

    shutil.rmtree(temp_dir, ignore_errors=True)


def test_extractor_synthetic_batch_schema():
    """Verify RedditExtractor creates properly structured submissions."""
    extractor = RedditExtractor()
    posts = extractor.generate_synthetic_batch(
        subreddits=["dataengineering", "machinelearning"],
        posts_per_subreddit=10,
        random_seed=42
    )

    assert len(posts) == 20
    required_keys = ["id", "title", "author", "subreddit", "score", "upvote_ratio", "num_comments", "created_utc"]
    for p in posts:
        for k in required_keys:
            assert k in p
        assert p["score"] >= 0
        assert 0.0 <= p["upvote_ratio"] <= 1.0
        assert p["num_comments"] >= 0


def test_extractor_bronze_partitioning(test_env):
    """Verify Bronze layer writes partitioned JSON directory structures."""
    extractor = RedditExtractor(bronze_dir=test_env["bronze"])
    posts = extractor.generate_synthetic_batch(
        subreddits=["dataengineering"],
        posts_per_subreddit=25,
        random_seed=101
    )
    written_files = extractor.persist_to_bronze(posts)

    assert len(written_files) > 0
    for f in written_files:
        assert f.exists()
        assert "year=" in str(f)
        assert "month=" in str(f)
        assert "day=" in str(f)


def test_transformer_cleaning_and_enrichment(test_env):
    """Verify Silver layer deduplication, enrichment, and schema adherence."""
    transformer = RedditTransformer(bronze_dir=test_env["bronze"], silver_dir=test_env["silver"])
    records, stats = transformer.load_bronze_records()
    assert len(records) == 25
    assert stats["corrupted_files"] == 0

    # Inject an intentional duplicate to test deduplication
    duplicate_record = records[0].copy()
    duplicate_record["score"] = 9999
    records.append(duplicate_record)

    df, dq_report = transformer.clean_and_enrich(records)

    # Assert deduplication succeeded
    assert len(df) == 25
    assert dq_report["duplicates_removed"] == 1
    assert dq_report["schema_valid"] is True
    assert dq_report["health_status"] in ["HEALTHY", "WARNING"]

    # Assert feature enrichments
    assert "engagement_score" in df.columns
    assert "comment_velocity_per_hour" in df.columns
    assert "title_word_count" in df.columns
    assert "subreddit_category" in df.columns

    # Verify Parquet persistence
    parquet_path = transformer.persist_to_silver(df)
    assert parquet_path.exists()
    assert parquet_path.stat().st_size > 0


def test_warehouse_dimensional_modeling(test_env):
    """Verify Gold layer Kimball Star Schema, views, and integrity."""
    silver_parquet = test_env["silver"] / "cleaned_posts.parquet"
    warehouse = RedditDataWarehouse(silver_path=silver_parquet, db_path=test_env["db_path"])
    result = warehouse.build_star_schema()

    assert result["status"] == "SUCCESS"
    counts = result["table_counts"]
    assert counts["dim_subreddit_count"] >= 1
    assert counts["dim_author_count"] >= 1
    assert counts["dim_time_count"] >= 1
    assert counts["fact_post_engagement_count"] == 25

    # Check foreign key referential integrity (no orphan rows)
    con = warehouse.get_connection()
    orphan_subs = con.execute("""
        SELECT COUNT(*) FROM fact_post_engagement f
        LEFT JOIN dim_subreddit s ON f.subreddit_id = s.subreddit_id
        WHERE s.subreddit_id IS NULL;
    """).fetchone()[0]
    assert orphan_subs == 0

    orphan_authors = con.execute("""
        SELECT COUNT(*) FROM fact_post_engagement f
        LEFT JOIN dim_author a ON f.author_id = a.author_id
        WHERE a.author_id IS NULL;
    """).fetchone()[0]
    assert orphan_authors == 0

    # Verify analytical marts return rows
    mart_sub = con.execute("SELECT * FROM mart_subreddit_performance;").df()
    assert len(mart_sub) >= 1
    assert "avg_engagement_score" in mart_sub.columns

    mart_auth = con.execute("SELECT * FROM mart_author_influence;").df()
    assert len(mart_auth) >= 1
    assert "total_engagement_score" in mart_auth.columns

    con.close()


def test_data_quality_gates_failure_on_high_nulls(test_env):
    """Verify Data Quality gate triggers WARNING when null rates violate contract."""
    transformer = RedditTransformer(bronze_dir=test_env["bronze"], silver_dir=test_env["silver"])
    records = [
        {"id": f"bad_{i}", "title": None, "author": None, "subreddit": "python", "score": None, "created_utc": 1600000000}
        for i in range(150)
    ]
    df, dq_report = transformer.clean_and_enrich(records)
    assert dq_report["health_status"] == "WARNING"
    assert dq_report["volume_threshold_passed"] is False


def test_visualizations_generation(test_env):
    """Verify all 5 publication visualization artifacts are rendered and non-empty."""
    from src.visualizations import PipelineVisualizer
    silver_parquet = test_env["silver"] / "cleaned_posts.parquet"
    warehouse = RedditDataWarehouse(silver_path=silver_parquet, db_path=test_env["db_path"])
    viz_dir = test_env["root"] / "artifacts"
    visualizer = PipelineVisualizer(warehouse=warehouse, artifacts_dir=viz_dir)

    dq_dummy = {"initial_records": 100, "deduplicated_records": 100, "null_rates_per_column": {}}
    artifacts = visualizer.generate_all_visualizations(dq_dummy)

    assert len(artifacts) == 5
    for key, path_str in artifacts.items():
        p = Path(path_str)
        assert p.exists()
        assert p.stat().st_size > 5000  # valid image file > 5KB
