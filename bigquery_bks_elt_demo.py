"""BigQuery ELT sample (Austin bikeshare) using the Rabbit Python SDK.

Standalone equivalent of the ``bigquery_elt_demo`` Airflow DAG, minus the GCS
export. Runs a two-step ELT against the
``bigquery-public-data.austin_bikeshare.bikeshare_trips`` public dataset:

  1. stage     - load a 30-day window into a managed staging table.
  2. aggregate - aggregate the staging table into a daily mart.

Each query job is submitted through :class:`RabbitBigQueryClient`, whose
``query()`` is a drop-in for ``bigquery.Client.query`` that first routes the job
configuration through the Rabbit Dynamic Pricing optimizer.

Configuration is read from environment variables:

  - ``GCP_PROJECT_ID`` (required)
  - ``BQ_DATASET``     (defaults to ``airflow_demo``)
  - ``BQ_LOCATION``    (defaults to ``US``)

The optimizer settings are read from environment variables here in the script
and passed to the client as an ``optimization_config`` dict:

  - ``RABBIT_RESERVATION_IDS``      comma-separated reservation IDs; empty -> skip
  - ``RABBIT_DEFAULT_PRICING_MODE`` ``on_demand`` (default) or ``slot_based``

The Rabbit API key and base URL are read by the SDK itself from
``RABBIT_API_KEY`` and ``RABBIT_API_BASE_URL`` (or pass ``api_key=`` /
``base_url=`` to ``RabbitBigQueryClient``).

Run with::

    GCP_PROJECT_ID=your-project python bigquery_bks_elt_demo.py
"""

from __future__ import annotations

import logging
import os

from rabbit_optimizer import RabbitBigQueryClient

logging.basicConfig(level=logging.INFO)


def _optimization_config() -> dict:
    """Build the Rabbit optimization config from environment variables."""
    reservation_ids = [
        rid.strip()
        for rid in os.environ.get("RABBIT_RESERVATION_IDS", "").split(",")
        if rid.strip()
    ]
    if not reservation_ids:
        return {}
    return {
        "default_pricing_mode": os.environ.get(
            "RABBIT_DEFAULT_PRICING_MODE", "on_demand"
        ),
        "reservation_ids": reservation_ids,
    }


def main() -> None:
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "airflow_demo")
    location = os.environ.get("BQ_LOCATION", "US")

    stage_table = f"{project}.{dataset}.stg_bikeshare_trips"
    mart_table = f"{project}.{dataset}.mart_daily_rides"

    stage_sql = f"""
DECLARE max_dt DATE DEFAULT (
  SELECT DATE(MAX(start_time))
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
);

CREATE OR REPLACE TABLE `{stage_table}` AS
SELECT
  trip_id,
  subscriber_type,
  bike_id,
  start_time,
  duration_minutes,
  start_station_id,
  start_station_name,
  end_station_id,
  end_station_name
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE DATE(start_time) BETWEEN DATE_SUB(max_dt, INTERVAL 30 DAY) AND max_dt
"""

    aggregate_sql = f"""
CREATE OR REPLACE TABLE `{mart_table}` AS
SELECT
  DATE(start_time)         AS ride_date,
  COUNT(*)                 AS rides,
  AVG(duration_minutes)    AS avg_duration_minutes,
  COUNT(DISTINCT bike_id)  AS unique_bikes
FROM `{stage_table}`
GROUP BY ride_date
ORDER BY ride_date
"""

    client = RabbitBigQueryClient(
        project=project, optimization_config=_optimization_config()
    )

    logging.info("Staging trips into %s", stage_table)
    client.query(stage_sql, location=location).result()

    logging.info("Aggregating daily rides into %s", mart_table)
    client.query(aggregate_sql, location=location).result()

    logging.info("Done.")


if __name__ == "__main__":
    main()
