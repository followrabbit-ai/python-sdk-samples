# python-sdk-samples

A collection of samples using the [Rabbit Dynamic Pricing Python SDK](https://pypi.org/project/rabbit-bq-job-optimizer/) (`rabbit-bq-job-optimizer`).

## BigQuery ELT samples

Each script runs a two-step ELT (stage -> aggregate) against a public dataset
and submits every query job through `RabbitBigQueryClient`, routing job
configuration through the Rabbit Dynamic Pricing optimizer.

| Script | Public dataset | Tables produced |
| --- | --- | --- |
| `bigquery_bks_elt_demo.py` | `austin_bikeshare.bikeshare_trips` | `stg_bikeshare_trips`, `mart_daily_rides` |
| `bigquery_bch_elt_demo.py` | `crypto_bitcoin_cash.transactions` | `stg_bch_transactions`, `mart_daily_bch_transactions` |

Similar pipelines are available as Airflow DAGs in
[`rabbit-sample-dags`](https://github.com/followrabbit-ai/rabbit-sample-dags).

### How the optimizer integration works

`rabbit_bigquery_client.py` provides `RabbitBigQueryClient`, a subclass of
`google.cloud.bigquery.Client` whose `query()` method is a drop-in for
`bigquery.Client.query`:

```python
from rabbit_bigquery_client import RabbitBigQueryClient

client = RabbitBigQueryClient(
    project="your-project",
    optimization_config={
        "default_pricing_mode": "on_demand",      # or "slot_based"
        "reservation_ids": ["your-project:US.your-reservation"],
    },
)
client.query(sql, location="US").result()
```

Before submitting, `query()` converts the job configuration to the API
representation, routes it through `RabbitBQJobOptimizer.optimize_job(...)`, and
rebuilds the optimized `QueryJobConfig`. The full `bigquery.Client.query`
signature is preserved, so `location`, `job_id`, `retry`, and other keyword
arguments still work. This uses the same optimization as the Rabbit Airflow
integration.

#### Configuration split

- **`api_key` / `base_url`** are constructor arguments forwarded to the SDK. If
  you leave them unset, the SDK resolves them itself: explicit argument ->
  `RABBIT_API_KEY` / `RABBIT_API_BASE_URL` environment variable -> default base
  URL. The samples rely on the environment variables.
- **`optimization_config`** is a plain, mutable dict holding the parameters the
  SDK does not read from the environment. Pass it to the constructor or omit it,
  and update it any time:

  ```python
  client.optimization_config["reservation_ids"] = ["your-project:US.your-reservation"]
  ```

  Keys: `default_pricing_mode` (`on_demand` default, or `slot_based`) and
  `reservation_ids` (a list). If `reservation_ids` is empty or the optimizer
  call fails, the query still runs -- just with its original, unoptimized
  configuration (graceful fallback).

## Prerequisites

1. Application Default Credentials:
   ```bash
   gcloud auth application-default login
   ```
2. A BigQuery dataset in your project to hold the staging and mart tables
   (defaults to `rabbit_demo`):
   ```bash
   bq --location=US mk --dataset "$GCP_PROJECT_ID:rabbit_demo"
   ```

## Install

Requires [uv](https://docs.astral.sh/uv/) (macOS, Linux, and Windows). Python 3.12 is pinned in `.python-version`.

Dependencies include the [Rabbit BigQuery Job Optimizer client](https://pypi.org/project/rabbit-bq-job-optimizer/) from PyPI (see [requirements.txt](requirements.txt)).

```bash
uv venv rabbit-python
uv pip install -r requirements.txt
```

Activate the virtual environment before running the demos: `source rabbit-python/bin/activate` (macOS/Linux) or `rabbit-python\Scripts\Activate.ps1` (Windows PowerShell).

**Alternative** (if you already have Python 3.11+ and prefer not to use uv):

```bash
python -m venv rabbit-python && source rabbit-python/bin/activate
pip install -r requirements.txt
```

On Windows, use `rabbit-python\Scripts\Activate.ps1` instead of `source`.

## Configure

The demo scripts take configuration as command-line options. Shared option
definitions live in `cli.py` and are reused by both scripts. Each option can also
be set via an environment variable instead; a flag on the command line takes
precedence when both are present.

| Option | Env var fallback | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--project` | `GCP_PROJECT_ID` | yes | -- | Project that owns the dataset and runs the jobs. |
| `--dataset` | `BQ_DATASET` | no | `rabbit_demo` | Dataset for the staging and mart tables. |
| `--location` | `BQ_LOCATION` | no | `US` | BigQuery location. |
| `--reservation-ids` | `RABBIT_RESERVATION_IDS` | no | -- | Comma-separated reservation IDs. Empty -> jobs run unoptimized. |
| `--pricing-mode` | `RABBIT_DEFAULT_PRICING_MODE` | no | `on_demand` | `on_demand` or `slot_based`. |

The scripts turn `--reservation-ids` and `--pricing-mode` into the
`optimization_config` dict described above.

The Rabbit API key and base URL are not CLI options. The SDK reads them from
`RABBIT_API_KEY` and `RABBIT_API_BASE_URL` (or you can pass `api_key=` /
`base_url=` to `RabbitBigQueryClient`).

Without `RABBIT_API_KEY` and `--reservation-ids`, the queries still run --
they are simply submitted with their original configuration (graceful fallback).

## Run

```bash
python bigquery_bks_elt_demo.py --help

python bigquery_bks_elt_demo.py --project your-project

python bigquery_bks_elt_demo.py \
    --project your-project \
    --dataset rabbit_demo \
    --location US \
    --pricing-mode slot_based \
    --reservation-ids your-project:US.your-reservation

python bigquery_bch_elt_demo.py --project your-project
```

Set the Rabbit API key via environment (not a CLI flag):

```bash
export RABBIT_API_KEY=your-rabbit-api-key
python bigquery_bks_elt_demo.py --project your-project --reservation-ids your-project:US.your-reservation
```

You can also supply the same settings via environment variables and run without
flags:

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
    "SELECT * FROM \`$GCP_PROJECT_ID.rabbit_demo.mart_daily_rides\` ORDER BY ride_date DESC LIMIT 10"
```
