"""
Reddit Lakehouse Transformer: Silver Layer Pipeline.
Reads partitioned Bronze JSON payloads, applies schema validation, deduplication,
data cleaning, and analytical feature enrichments, then writes optimized columnar Parquet.
Tracks comprehensive data quality health metrics.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    BRONZE_DIR,
    SILVER_DIR,
    SUBREDDIT_METADATA,
    MAX_ALLOWED_NULL_PERCENT,
    MIN_REQUIRED_RECORDS
)


class RedditTransformer:
    """
    Transforms Bronze raw JSON into cleansed, validated Silver Parquet dataset.
    """

    REQUIRED_RAW_FIELDS = [
        "id", "title", "author", "subreddit", "score",
        "upvote_ratio", "num_comments", "created_utc"
    ]

    def __init__(self, bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR):
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.silver_dir.mkdir(parents=True, exist_ok=True)

    def load_bronze_records(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Recursively scans Bronze Lakehouse directory and aggregates raw JSON payloads.
        """
        json_files = list(self.bronze_dir.glob("**/*.json"))
        records = []
        corrupted_files = 0

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        records.extend(data)
                    elif isinstance(data, dict):
                        records.append(data)
            except Exception:
                corrupted_files += 1

        stats = {
            "total_files_scanned": len(json_files),
            "corrupted_files": corrupted_files,
            "raw_record_count": len(records)
        }
        return records, stats

    def clean_and_enrich(self, records: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Cleans, deduplicates, validates, and enriches raw records.
        """
        if not records:
            raise ValueError("No records found in Bronze layer to transform.")

        df = pd.DataFrame(records)
        initial_count = len(df)

        # Check raw schema presence before imputation
        passed_schema = all(col in df.columns for col in self.REQUIRED_RAW_FIELDS)

        # Ensure required columns exist even in dirty batches
        for col in self.REQUIRED_RAW_FIELDS:
            if col not in df.columns:
                df[col] = None

        if "total_awards_received" not in df.columns:
            df["total_awards_received"] = 0
        if "selftext" not in df.columns:
            df["selftext"] = ""

        # 1. Deduplication by post ID (keeping highest score / latest)
        if "score" in df.columns:
            df["score_temp"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
            df = df.sort_values(by=["score_temp"], ascending=False).drop_duplicates(subset=["id"], keep="first").drop(columns=["score_temp"])
        else:
            df = df.drop_duplicates(subset=["id"], keep="first")
            
        dedup_count = len(df)
        duplicates_removed = initial_count - dedup_count

        # 2. Schema normalization & string cleaning
        df["title"] = df["title"].fillna("").astype(str).str.strip()
        df["subreddit"] = df["subreddit"].fillna("unknown").astype(str).str.strip().str.lower()
        
        # Author normalization: handle deleted accounts
        df["is_deleted_author"] = df["author"].isna() | df["author"].isin(["[deleted]", "None", "", None])
        df["author"] = df["author"].apply(lambda x: "anonymous_user" if (pd.isna(x) or str(x) in ["[deleted]", "None", "", "nan"]) else str(x))

        # 3. Numeric bounds enforcement
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
        df["score"] = df["score"].clip(lower=0)
        
        df["num_comments"] = pd.to_numeric(df["num_comments"], errors="coerce").fillna(0).astype(int)
        df["num_comments"] = df["num_comments"].clip(lower=0)

        df["upvote_ratio"] = pd.to_numeric(df["upvote_ratio"], errors="coerce").fillna(0.50).astype(float)
        df["upvote_ratio"] = df["upvote_ratio"].clip(lower=0.0, upper=1.0)

        df["total_awards_received"] = pd.to_numeric(df["total_awards_received"], errors="coerce").fillna(0).astype(int)
        df["total_awards_received"] = df["total_awards_received"].clip(lower=0)

        # 4. Temporal parsing and calendar features
        df["created_utc"] = pd.to_numeric(df["created_utc"], errors="coerce").fillna(int(datetime.now(timezone.utc).timestamp())).astype(int)
        created_dt = pd.to_datetime(df["created_utc"], unit="s", utc=True)
        df["created_timestamp"] = created_dt
        df["created_date"] = created_dt.dt.date.astype(str)
        df["created_year"] = created_dt.dt.year
        df["created_month"] = created_dt.dt.month
        df["created_day"] = created_dt.dt.day
        df["created_hour"] = created_dt.dt.hour
        df["created_day_of_week"] = created_dt.dt.day_name()
        df["day_of_week_num"] = created_dt.dt.dayofweek
        df["is_weekend"] = df["day_of_week_num"] >= 5

        # 5. Enrichment: Text and Engagement Velocity metrics
        df["selftext"] = df["selftext"].fillna("").astype(str)
        df["selftext_length"] = df["selftext"].str.len()
        df["has_selftext"] = df["selftext_length"] > 0
        df["title_word_count"] = df["title"].str.split().str.len()
        df["title_char_count"] = df["title"].str.len()

        # Age and comment velocity (relative to latest post timestamp)
        max_time = df["created_timestamp"].max()
        df["post_age_hours"] = ((max_time - df["created_timestamp"]).dt.total_seconds() / 3600.0).clip(lower=1.0)
        df["comment_velocity_per_hour"] = (df["num_comments"] / df["post_age_hours"]).round(3)
        df["comment_to_score_ratio"] = (df["num_comments"] / np.maximum(1, df["score"])).round(4)
        
        # Compound Engagement Score metric
        # Engagement = (Score * UpvoteRatio) + (Comments * 2.5) + (Awards * 10)
        df["engagement_score"] = (
            (df["score"] * df["upvote_ratio"]) +
            (df["num_comments"] * 2.5) +
            (df["total_awards_received"] * 10.0)
        ).round(2)

        # Subreddit category enrichment
        df["subreddit_category"] = df["subreddit"].apply(
            lambda s: SUBREDDIT_METADATA.get(s, {}).get("category", "General Tech")
        )
        df["subscriber_tier"] = df["subreddit"].apply(
            lambda s: SUBREDDIT_METADATA.get(s, {}).get("subscriber_tier", "Standard")
        )

        # 6. Data Quality Checks & Health Scorecard
        null_counts = df.isnull().sum().to_dict()
        null_percentages = {k: round(v / len(df), 4) for k, v in null_counts.items()}
        max_null_pct = max(null_percentages.values())
        
        passed_null_check = max_null_pct <= MAX_ALLOWED_NULL_PERCENT
        passed_volume_check = len(df) >= MIN_REQUIRED_RECORDS

        dq_report = {
            "initial_records": initial_count,
            "deduplicated_records": dedup_count,
            "duplicates_removed": duplicates_removed,
            "schema_valid": passed_schema,
            "volume_threshold_passed": passed_volume_check,
            "null_threshold_passed": passed_null_check,
            "max_column_null_rate": max_null_pct,
            "null_rates_per_column": null_percentages,
            "health_status": "HEALTHY" if (passed_schema and passed_null_check and passed_volume_check) else "WARNING"
        }

        return df, dq_report

    def persist_to_silver(self, df: pd.DataFrame) -> Path:
        """
        Saves cleaned DataFrame as an optimized Parquet table.
        """
        output_file = self.silver_dir / "cleaned_posts.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, compression="SNAPPY")
        return output_file

    def run_transformation(self) -> Dict[str, Any]:
        """
        Executes end-to-end Silver transformation pipeline.
        """
        records, load_stats = self.load_bronze_records()
        df, dq_report = self.clean_and_enrich(records)
        output_path = self.persist_to_silver(df)

        return {
            "status": "SUCCESS",
            "silver_output_file": str(output_path),
            "load_stats": load_stats,
            "data_quality_report": dq_report,
            "record_count": len(df)
        }


if __name__ == "__main__":
    transformer = RedditTransformer()
    res = transformer.run_transformation()
    print("Silver Transformation Result Summary:")
    print("Record Count:", res["record_count"])
    print("Data Quality Health:", res["data_quality_report"]["health_status"])
