"""
Reddit Lakehouse Warehouse: Gold Layer Star Schema Modeling.
Implements Kimball dimensional modeling using DuckDB as an embedded OLAP engine.
Constructs dim_subreddit, dim_author, dim_time, and fact_post_engagement,
along with analytical data marts. Exports to DuckDB and Parquet tables.
"""

from pathlib import Path
from typing import Dict, Any
import duckdb

from src.config import SILVER_DIR, GOLD_DIR, DUCKDB_DATABASE_PATH


class RedditDataWarehouse:
    """
    Manages Gold Layer dimensional data warehouse and analytical aggregations using DuckDB.
    """

    def __init__(self, silver_path: Path = SILVER_DIR / "cleaned_posts.parquet", db_path: Path = DUCKDB_DATABASE_PATH):
        self.silver_path = silver_path
        self.db_path = db_path
        self.gold_dir = self.db_path.parent
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Creates or returns connection to persistent DuckDB database file.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(database=str(self.db_path))

    def build_star_schema(self) -> Dict[str, Any]:
        """
        Executes SQL DDL & DML to build star schema and analytical marts from Silver Parquet.
        """
        con = self.get_connection()
        try:
            silver_file = str(self.silver_path)

            # 1. Dimension: Subreddit
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_subreddit AS
                SELECT
                    ROW_NUMBER() OVER (ORDER BY subreddit) AS subreddit_id,
                    subreddit AS subreddit_name,
                    FIRST(subreddit_category) AS category,
                    FIRST(subscriber_tier) AS subscriber_tier
                FROM read_parquet('{silver_file}')
                GROUP BY subreddit;
            """)

            # 2. Dimension: Author
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_author AS
                SELECT
                    ROW_NUMBER() OVER (ORDER BY author) AS author_id,
                    author AS author_name,
                    BOOL_OR(is_deleted_author) AS is_anonymous,
                    MIN(created_utc) AS first_activity_utc,
                    COUNT(*) AS total_posts_count,
                    SUM(score) AS total_score,
                    CASE
                        WHEN COUNT(*) >= 50 THEN 'Power Contributor'
                        WHEN COUNT(*) >= 20 THEN 'Active Poster'
                        ELSE 'Occasional Poster'
                    END AS author_tier
                FROM read_parquet('{silver_file}')
                GROUP BY author;
            """)

            # 3. Dimension: Time (Calendar Date)
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_time AS
                SELECT DISTINCT
                    CAST(strftime(created_timestamp, '%Y%m%d') AS INTEGER) AS date_key,
                    created_date AS full_date,
                    created_year AS year,
                    created_month AS month,
                    created_day AS day,
                    created_day_of_week AS day_of_week,
                    day_of_week_num,
                    is_weekend
                FROM read_parquet('{silver_file}')
                ORDER BY full_date;
            """)

            # 4. Fact Table: Post Engagement
            con.execute(f"""
                CREATE OR REPLACE TABLE fact_post_engagement AS
                SELECT
                    p.id AS post_id,
                    s.subreddit_id,
                    a.author_id,
                    CAST(strftime(p.created_timestamp, '%Y%m%d') AS INTEGER) AS date_key,
                    p.created_hour,
                    p.score,
                    p.num_comments,
                    p.upvote_ratio,
                    p.engagement_score,
                    p.comment_velocity_per_hour,
                    p.comment_to_score_ratio,
                    p.title_word_count,
                    p.selftext_length,
                    p.has_selftext,
                    p.total_awards_received,
                    p.is_deleted_author
                FROM read_parquet('{silver_file}') p
                INNER JOIN dim_subreddit s ON p.subreddit = s.subreddit_name
                INNER JOIN dim_author a ON p.author = a.author_name;
            """)

            # 5. Analytical Mart 1: Subreddit Performance Daily Aggregation
            con.execute("""
                CREATE OR REPLACE VIEW mart_subreddit_performance AS
                SELECT
                    s.subreddit_name,
                    s.category,
                    COUNT(f.post_id) AS total_posts,
                    ROUND(AVG(f.score), 2) AS avg_score,
                    MEDIAN(f.score) AS median_score,
                    ROUND(AVG(f.upvote_ratio), 3) AS avg_upvote_ratio,
                    SUM(f.num_comments) AS total_comments,
                    ROUND(AVG(f.num_comments), 2) AS avg_comments_per_post,
                    ROUND(AVG(f.comment_velocity_per_hour), 3) AS avg_comment_velocity,
                    ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score,
                    ROUND(SUM(CASE WHEN f.has_selftext THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS selftext_post_pct
                FROM fact_post_engagement f
                JOIN dim_subreddit s ON f.subreddit_id = s.subreddit_id
                GROUP BY s.subreddit_name, s.category
                ORDER BY total_posts DESC;
            """)

            # 6. Analytical Mart 2: Author Influence Ranking
            con.execute("""
                CREATE OR REPLACE VIEW mart_author_influence AS
                SELECT
                    a.author_name,
                    a.author_tier,
                    COUNT(f.post_id) AS posts_published,
                    SUM(f.score) AS cumulative_score,
                    SUM(f.num_comments) AS cumulative_comments,
                    ROUND(AVG(f.score), 2) AS avg_score_per_post,
                    ROUND(AVG(f.upvote_ratio), 3) AS avg_upvote_ratio,
                    ROUND(SUM(f.engagement_score), 2) AS total_engagement_score
                FROM fact_post_engagement f
                JOIN dim_author a ON f.author_id = a.author_id
                WHERE a.is_anonymous = FALSE
                GROUP BY a.author_name, a.author_tier
                ORDER BY total_engagement_score DESC;
            """)

            # 7. Analytical Mart 3: Temporal Heatmap Data
            con.execute("""
                CREATE OR REPLACE VIEW mart_temporal_patterns AS
                SELECT
                    t.day_of_week,
                    t.day_of_week_num,
                    f.created_hour,
                    COUNT(f.post_id) AS post_volume,
                    ROUND(AVG(f.score), 2) AS avg_score,
                    ROUND(AVG(f.num_comments), 2) AS avg_comments,
                    ROUND(AVG(f.engagement_score), 2) AS avg_engagement
                FROM fact_post_engagement f
                JOIN dim_time t ON f.date_key = t.date_key
                GROUP BY t.day_of_week, t.day_of_week_num, f.created_hour
                ORDER BY t.day_of_week_num, f.created_hour;
            """)

            # Export Gold Layer tables to Parquet for lakehouse interoperability
            tables_to_export = [
                ("dim_subreddit", self.gold_dir / "dim_subreddit.parquet"),
                ("dim_author", self.gold_dir / "dim_author.parquet"),
                ("dim_time", self.gold_dir / "dim_time.parquet"),
                ("fact_post_engagement", self.gold_dir / "fact_post_engagement.parquet"),
                ("mart_subreddit_performance", self.gold_dir / "mart_subreddit_performance.parquet"),
                ("mart_author_influence", self.gold_dir / "mart_author_influence.parquet"),
                ("mart_temporal_patterns", self.gold_dir / "mart_temporal_patterns.parquet")
            ]

            exported_files = {}
            for tbl_name, file_path in tables_to_export:
                con.execute(f"COPY {tbl_name} TO '{file_path}' (FORMAT PARQUET);")
                exported_files[tbl_name] = str(file_path)

            # Metadata counts
            counts = {
                "dim_subreddit_count": con.execute("SELECT COUNT(*) FROM dim_subreddit").fetchone()[0],
                "dim_author_count": con.execute("SELECT COUNT(*) FROM dim_author").fetchone()[0],
                "dim_time_count": con.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0],
                "fact_post_engagement_count": con.execute("SELECT COUNT(*) FROM fact_post_engagement").fetchone()[0]
            }

            return {
                "status": "SUCCESS",
                "database_path": str(self.db_path),
                "table_counts": counts,
                "exported_parquet_files": exported_files
            }

        finally:
            con.close()

    def query(self, sql_query: str):
        """
        Runs an ad-hoc SQL query against the Gold warehouse and returns a Pandas DataFrame.
        """
        con = self.get_connection()
        try:
            return con.execute(sql_query).df()
        finally:
            con.close()


if __name__ == "__main__":
    warehouse = RedditDataWarehouse()
    result = warehouse.build_star_schema()
    print("Gold Layer Warehouse Build Result:")
    for k, v in result["table_counts"].items():
        print(f"  {k}: {v}")
