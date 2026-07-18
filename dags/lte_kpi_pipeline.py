"""
LTE KPI Pipeline: Extract -> Transform (Pandas) -> Load (BigQuery)

This is the "real data" version of the earlier bq_sample_pipeline demo DAG.
It reads your sanitized synthetic LTE dataset (vendors, markets, sites,
network_kpi_daily), joins them together, computes RRC success rate per
site/day, flags underperforming sites against a threshold, and loads the
enriched result into BigQuery.

This mirrors the real logic of the LTE Performance ETL & Trending Analytics
Platform: daily KPI monitoring + threshold-based exception detection +
worst-cell prioritization.

SETUP NOTES (read before running):
1. Copy the 4 CSV files (vendors.csv, markets.csv, sites.csv,
   network_kpi_daily.csv) into: C:\\Users\\mgrig\\airflow-docker\\dags\\data\\
2. PROJECT_ID / DATASET_ID below are already set to your BigQuery Sandbox.
3. Credentials are expected at /opt/airflow/config/bq-key.json (already
   mounted via docker-compose, same as the earlier DAG).
"""

from __future__ import annotations

import os
import pandas as pd
from datetime import datetime

from airflow.sdk import dag, task

# --- CONFIG ---
PROJECT_ID = "python-gsheets-learning"
DATASET_ID = "airflow_demo"
TABLE_ID = "lte_kpi_enriched"
RRC_THRESHOLD = 98.0  # success rate % below this is flagged
DATA_DIR = "/opt/airflow/dags/data"
# --------------

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/bq-key.json"


@dag(
    dag_id="lte_kpi_pipeline",
    schedule="@daily",  # runs once every 24 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,  # don't backfill runs for past dates when first enabled
    tags=["portfolio", "bigquery", "lte"],
)
def lte_kpi_pipeline():

    @task
    def extract() -> dict:
        """Read the four source CSVs."""
        vendors = pd.read_csv(f"{DATA_DIR}/vendors.csv")
        markets = pd.read_csv(f"{DATA_DIR}/markets.csv")
        sites = pd.read_csv(f"{DATA_DIR}/sites.csv")
        kpi_daily = pd.read_csv(f"{DATA_DIR}/network_kpi_daily.csv")

        # Stash each as its own CSV for the next task to re-read
        # (keeps tasks decoupled, mirrors how a real pipeline would
        # pass data via an intermediate store rather than in-memory)
        vendors.to_csv(f"{DATA_DIR}/_stage_vendors.csv", index=False)
        markets.to_csv(f"{DATA_DIR}/_stage_markets.csv", index=False)
        sites.to_csv(f"{DATA_DIR}/_stage_sites.csv", index=False)
        kpi_daily.to_csv(f"{DATA_DIR}/_stage_kpi_daily.csv", index=False)

        return {"row_count": len(kpi_daily)}

    @task
    def transform(extract_info: dict) -> str:
        """Join dimensions onto the KPI fact table, compute metrics, flag exceptions."""
        vendors = pd.read_csv(f"{DATA_DIR}/_stage_vendors.csv")
        markets = pd.read_csv(f"{DATA_DIR}/_stage_markets.csv")
        sites = pd.read_csv(f"{DATA_DIR}/_stage_sites.csv")
        kpi = pd.read_csv(f"{DATA_DIR}/_stage_kpi_daily.csv", parse_dates=["report_date"])

        # Join sites -> vendors, sites -> markets
        sites_enriched = sites.merge(vendors, on="vendor_id", how="left")
        sites_enriched = sites_enriched.merge(markets, on="market_id", how="left")

        # Join KPI facts -> enriched site dimension
        df = kpi.merge(sites_enriched, on="site_id", how="left")

        # Core metric: RRC success rate
        df["rrc_success_rate"] = (df["rrc_completions"] / df["rrc_attempts"]) * 100
        df["rrc_success_rate"] = df["rrc_success_rate"].round(2)

        # Threshold-based exception flag
        df["below_threshold"] = df["rrc_success_rate"] < RRC_THRESHOLD

        # 7-day rolling average per site (trend analysis)
        df = df.sort_values(["site_id", "report_date"])
        df["rrc_success_rate_7d_avg"] = (
            df.groupby("site_id")["rrc_success_rate"]
            .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
            .round(2)
        )

        df["load_ts"] = datetime.utcnow().isoformat()

        output_path = f"{DATA_DIR}/_stage_transformed.csv"
        df.to_csv(output_path, index=False)
        print(f"Transformed {len(df)} rows. {df['below_threshold'].sum()} below threshold.")
        return output_path

    @task
    def load(input_path: str) -> None:
        """Load the enriched dataset into BigQuery."""
        from google.cloud import bigquery

        df = pd.read_csv(input_path, parse_dates=["report_date"])

        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"Loaded {len(df)} rows into {table_ref}")

    info = extract()
    transformed_path = transform(info)
    load(transformed_path)


lte_kpi_pipeline()
