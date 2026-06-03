"""BigQuery ELT sample (Bitcoin Cash) using the Rabbit Python SDK.

Standalone equivalent of the ``bigquery_bch_elt_demo`` Airflow DAG, minus the
GCS export. Runs a two-step ELT against
``bigquery-public-data.crypto_bitcoin_cash.transactions``:

  1. stage     - load a 30-day window into a managed staging table.
  2. aggregate - daily aggregates into a mart table.

Uses ``block_timestamp_month`` in predicates where possible to limit scanned
partitions (the public table is large).

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

    GCP_PROJECT_ID=your-project python bigquery_bch_elt_demo.py
"""

from __future__ import annotations

import logging
import os

from rabbit_optimizer import RabbitBigQueryClient

logging.basicConfig(level=logging.INFO)

PUBLIC_TX = "`bigquery-public-data.crypto_bitcoin_cash.transactions`"


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
        project=project, optimization_config=_optimization_config()
    )

    logging.info("Staging transactions into %s", stage_table)
    client.query(stage_sql, location=location).result()

    logging.info("Aggregating daily transactions into %s", mart_table)
    client.query(aggregate_sql, location=location).result()

    logging.info("Done.")


if __name__ == "__main__":
    main()
