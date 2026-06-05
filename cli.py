"""Shared Typer CLI options and configuration helpers for the BigQuery ELT samples.

The demo scripts reuse the ``typer.Option`` instances defined here as their
parameter defaults, so the command-line interface is declared once. Each option
can also be set via an environment variable; a flag takes precedence when both
are present:

  - ``--project``         / ``GCP_PROJECT_ID``              (required)
  - ``--dataset``         / ``BQ_DATASET``                  (defaults to ``rabbit_demo``)
  - ``--location``        / ``BQ_LOCATION``                 (defaults to ``US``)
  - ``--pricing-mode``    / ``RABBIT_DEFAULT_PRICING_MODE`` (``on_demand`` or ``slot_based``)
  - ``--reservation-ids`` / ``RABBIT_RESERVATION_IDS``      (comma-separated; empty -> unoptimized)

The Rabbit API key and base URL are intentionally not exposed as options. The
SDK reads them itself from ``RABBIT_API_KEY`` and ``RABBIT_API_BASE_URL``.
"""

from enum import Enum

import typer


class PricingMode(str, Enum):
    on_demand = "on_demand"
    slot_based = "slot_based"


PROJECT = typer.Option(
    ...,
    envvar="GCP_PROJECT_ID",
    show_envvar=True,
    help="GCP project that owns the dataset and runs the jobs.",
)
DATASET = typer.Option(
    "rabbit_demo",
    envvar="BQ_DATASET",
    show_envvar=True,
    help="Dataset for the staging and mart tables.",
)
LOCATION = typer.Option(
    "US",
    envvar="BQ_LOCATION",
    show_envvar=True,
    help="BigQuery location.",
)
PRICING_MODE = typer.Option(
    PricingMode.on_demand,
    envvar="RABBIT_DEFAULT_PRICING_MODE",
    show_envvar=True,
    help="Rabbit default pricing mode.",
)
RESERVATION_IDS = typer.Option(
    "",
    envvar="RABBIT_RESERVATION_IDS",
    show_envvar=True,
    help="Comma-separated reservation IDs. Empty -> jobs run unoptimized.",
)


def build_optimization_config(pricing_mode: PricingMode, reservation_ids: str) -> dict:
    """Build the Rabbit optimization config from the resolved CLI options."""
    ids = [r.strip() for r in reservation_ids.split(",") if r.strip()]
    if not ids:
        return {}
    return {
        "default_pricing_mode": pricing_mode.value,
        "reservation_ids": ids,
    }
