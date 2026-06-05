"""BigQuery ELT sample (Austin bikeshare) using the Rabbit Python SDK.

Standalone equivalent of the ``bigquery_elt_demo`` Airflow DAG, minus the GCS
export. Runs a two-step ELT against the
``bigquery-public-data.austin_bikeshare.bikeshare_trips`` public dataset:

  1. stage     - load a 30-day window into a managed staging table.
  2. aggregate - aggregate the staging table into a daily mart.

Each query job is submitted through :class:`RabbitBigQueryClient`, whose
``query()`` is a drop-in for ``bigquery.Client.query`` that first routes the job
configuration through the Rabbit Dynamic Pricing optimizer.

Configuration is passed as command-line options (see ``cli.py``). Each option
can also be set via an environment variable; a flag takes precedence when both
are present:

  - ``--project``         / ``GCP_PROJECT_ID``              (required)
  - ``--dataset``         / ``BQ_DATASET``                  (defaults to ``airflow_demo``)
  - ``--location``        / ``BQ_LOCATION``                 (defaults to ``US``)
  - ``--pricing-mode``    / ``RABBIT_DEFAULT_PRICING_MODE`` (``on_demand`` or ``slot_based``)
  - ``--reservation-ids`` / ``RABBIT_RESERVATION_IDS``      (comma-separated; empty -> unoptimized)

The Rabbit API key and base URL are read by the SDK itself from
``RABBIT_API_KEY`` and ``RABBIT_API_BASE_URL``.

Run with::

    python bigquery_bks_elt_demo.py --project your-project

See ``python bigquery_bks_elt_demo.py --help`` for all options.
"""

import logging

import typer

from cli import (
    DATASET,
    LOCATION,
    PRICING_MODE,
    PROJECT,
    RESERVATION_IDS,
    PricingMode,
    build_optimization_config,
)
from rabbit_optimizer import RabbitBigQueryClient

logging.basicConfig(level=logging.INFO)

app = typer.Typer(add_completion=False)


@app.command()
def main(
    project: str = PROJECT,
    dataset: str = DATASET,
    location: str = LOCATION,
    pricing_mode: PricingMode = PRICING_MODE,
    reservation_ids: str = RESERVATION_IDS,
) -> None:
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
        project=project,
        optimization_config=build_optimization_config(pricing_mode, reservation_ids),
    )

    logging.info("Staging trips into %s", stage_table)
    client.query(stage_sql, location=location).result()

    logging.info("Aggregating daily rides into %s", mart_table)
    client.query(aggregate_sql, location=location).result()

    logging.info("Done.")


if __name__ == "__main__":
    app()
