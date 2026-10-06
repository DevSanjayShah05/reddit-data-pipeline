"""
Configuration module for the Reddit Lakehouse Data Engineering Pipeline.
Defines directory paths, schema definitions, subreddit targets, and operational parameters.
"""

from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "lakehouse"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

# Ensure runtime directories exist
for path in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, ARTIFACTS_DIR, NOTEBOOKS_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# Reddit Scraping / Ingestion Parameters
TARGET_SUBREDDITS = [
    "dataengineering",
    "machinelearning",
    "datascience",
    "technology",
    "python"
]

SUBREDDIT_METADATA = {
    "dataengineering": {"category": "Data & Infra", "subscriber_tier": "Large"},
    "machinelearning": {"category": "AI & Research", "subscriber_tier": "Mega"},
    "datascience": {"category": "Analytics & Statistics", "subscriber_tier": "Large"},
    "technology": {"category": "Tech News & Discussion", "subscriber_tier": "Mega"},
    "python": {"category": "Programming & Languages", "subscriber_tier": "Mega"}
}

# Data Quality Thresholds
MAX_ALLOWED_NULL_PERCENT = 0.05
MIN_REQUIRED_RECORDS = 1000
DUCKDB_DATABASE_PATH = GOLD_DIR / "reddit_warehouse.duckdb"
