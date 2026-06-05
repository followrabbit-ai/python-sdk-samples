"""BigQuery ELT sample (Bitcoin Cash) using the Rabbit Python SDK.

Runs a two-step ELT against
``bigquery-public-data.crypto_bitcoin_cash.transactions``:

  1. stage     - load a 30-day window into a managed staging table.
  2. aggregate - daily aggregates into a mart table.

Uses ``block_timestamp_month`` in predicates where possible to limit scanned
partitions (the public table is large).

Each query job is submitted through :class:`RabbitBigQueryClient`, whose
``query()`` is a drop-in for ``bigquery.Client.query`` that first routes the job
configuration through the Rabbit Dynamic Pricing optimizer.

Configuration is passed as command-line options (see ``cli.py``). Each option
can also be set via an environment variable; a flag takes precedence when both
are present:

  - ``--project``         / ``GCP_PROJECT_ID``              (required)
  - ``--dataset``         / ``BQ_DATASET``                  (defaults to ``rabbit_demo``)
  - ``--location``        / ``BQ_LOCATION``                 (defaults to ``US``)
  - ``--pricing-mode``    / ``RABBIT_DEFAULT_PRICING_MODE`` (``on_demand`` or ``slot_based``)
  - ``--reservation-ids`` / ``RABBIT_RESERVATION_IDS``      (comma-separated; empty -> unoptimized)

The Rabbit API key and base URL are read by the SDK itself from
``RABBIT_API_KEY`` and ``RABBIT_API_BASE_URL``.

Run with::

    python bigquery_bch_elt_demo.py --project your-project

See ``python bigquery_bch_elt_demo.py --help`` for all options.

A similar pipeline exists as an Airflow DAG in rabbit-sample-dags.
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
from rabbit_bigquery_client import RabbitBigQueryClient

logging.basicConfig(level=logging.INFO)

PUBLIC_TX = "`bigquery-public-data.crypto_bitcoin_cash.transactions`"

app = typer.Typer(add_completion=False)


@app.command()
def main(
    project: str = PROJECT,
    dataset: str = DATASET,
    location: str = LOCATION,
    pricing_mode: PricingMode = PRICING_MODE,
    reservation_ids: str = RESERVATION_IDS,
) -> None:
    stage_table = f"{project}.{dataset}.stg_bch_transactions"
    mart_table = f"{project}.{dataset}.mart_daily_bch_transactions"

    stage_sql = f"""
DECLARE max_dt DATE DEFAULT (
  SELECT DATE(MAX(block_timestamp))
  FROM {PUBLIC_TX}
  WHERE block_timestamp_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 420 DAY)
);

CREATE OR REPLACE TABLE `{stage_table}` AS
SELECT
  `hash`,
  size,
  virtual_size,
  version,
  block_number,
  block_hash,
  block_timestamp,
  input_count,
  output_count,
  input_value,
  output_value,
  is_coinbase,
  fee
FROM {PUBLIC_TX}
WHERE block_timestamp_month >= DATE_SUB(max_dt, INTERVAL 70 DAY)
  AND DATE(block_timestamp) BETWEEN DATE_SUB(max_dt, INTERVAL 30 DAY) AND max_dt
"""

    aggregate_sql = f"""
CREATE OR REPLACE TABLE `{mart_table}` AS
SELECT
  DATE(block_timestamp)     AS tx_date,
  COUNT(*)                  AS tx_count,
  SUM(input_value)          AS total_input_value,
  SUM(output_value)         AS total_output_value,
  AVG(output_count)         AS avg_output_count,
  COUNTIF(is_coinbase)      AS coinbase_tx_count
FROM `{stage_table}`
GROUP BY tx_date
ORDER BY tx_date
"""

    client = RabbitBigQueryClient(
        project=project,
        optimization_config=build_optimization_config(pricing_mode, reservation_ids),
    )

    logging.info("Staging transactions into %s", stage_table)
    client.query(stage_sql, location=location).result()

    logging.info("Aggregating daily transactions into %s", mart_table)
    client.query(aggregate_sql, location=location).result()

    logging.info("Done.")


if __name__ == "__main__":
    app()
