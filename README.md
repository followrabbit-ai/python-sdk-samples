# python-sdk-samples

A collection of samples using the Rabbit Dynamic Pricing Python SDK.

## BigQuery ELT samples

Standalone Python equivalents of the
[`rabbit-sample-dags`](https://github.com/followrabbit-ai/rabbit-sample-dags)
Airflow DAGs, minus the Airflow orchestration and GCS export. Each script runs a
two-step ELT (stage -> aggregate) and submits every BigQuery query job through
the Rabbit Dynamic Pricing optimizer.

| Script | Public dataset | Tables produced |
| --- | --- | --- |
| `bigquery_bks_elt_demo.py` | `austin_bikeshare.bikeshare_trips` | `stg_bikeshare_trips`, `mart_daily_rides` |
| `bigquery_bch_elt_demo.py` | `crypto_bitcoin_cash.transactions` | `stg_bch_transactions`, `mart_daily_bch_transactions` |

### How the optimizer integration works

`rabbit_optimizer.py` provides `RabbitBigQueryClient`, a subclass of
`google.cloud.bigquery.Client` whose `query()` method is a drop-in for
`bigquery.Client.query`:

```python
from rabbit_optimizer import RabbitBigQueryClient

client = RabbitBigQueryClient(project="your-project")
client.query(sql, location="US").result()
```

Before submitting, `query()` converts the job configuration to the API
representation, routes it through `RabbitBQJobOptimizer.optimize_job(...)`, and
rebuilds the optimized `QueryJobConfig` -- the same flow the upstream Airflow
plugin applies by patching `BigQueryHook.insert_job`. The full
`bigquery.Client.query` signature is preserved, so `location`, `job_id`,
`retry`, and other keyword arguments still work.

## Prerequisites

1. Application Default Credentials:
   ```bash
   gcloud auth application-default login
   ```
2. A BigQuery dataset in your project to hold the staging and mart tables
   (defaults to `airflow_demo`):
   ```bash
   bq --location=US mk --dataset "$GCP_PROJECT_ID:airflow_demo"
   ```

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Configure

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GCP_PROJECT_ID` | yes | -- | Project that owns the dataset and runs the jobs. |
| `BQ_DATASET` | no | `airflow_demo` | Dataset for the staging and mart tables. |
| `BQ_LOCATION` | no | `US` | BigQuery location. |
| `RABBIT_API_KEY` | no | -- | Rabbit API key. Absent -> jobs run unoptimized. |
| `RABBIT_API_BASE_URL` | no | SDK default | Override the Rabbit API base URL. |
| `RABBIT_DEFAULT_PRICING_MODE` | no | `on_demand` | `on_demand` or `slot_based`. |
| `RABBIT_RESERVATION_IDS` | no | -- | Comma-separated reservation IDs. Empty -> jobs run unoptimized. |

Without `RABBIT_API_KEY` and `RABBIT_RESERVATION_IDS`, the queries still run --
they are simply submitted with their original configuration (matching the
plugin's graceful fallback).

## Run

```bash
export GCP_PROJECT_ID=your-project
export RABBIT_API_KEY=your-rabbit-api-key
export RABBIT_RESERVATION_IDS=your-project:US.your-reservation

python bigquery_bks_elt_demo.py
python bigquery_bch_elt_demo.py
```

## Verify

```bash
bq query --use_legacy_sql=false \
    "SELECT * FROM \`$GCP_PROJECT_ID.airflow_demo.mart_daily_rides\` ORDER BY ride_date DESC LIMIT 10"
```
