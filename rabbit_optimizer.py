"""A drop-in ``bigquery.Client`` whose ``query()`` is optimized by Rabbit.

``RabbitBigQueryClient`` subclasses :class:`google.cloud.bigquery.Client` and
overrides :meth:`query` so call sites are byte-for-byte the BigQuery API, but
every query job is transparently routed through the Rabbit Dynamic Pricing
optimizer first. This mirrors what the upstream Airflow plugin does by patching
``BigQueryHook.insert_job``.

Optimizer settings are read from environment variables. If any required setting
is missing, or the optimizer call fails for any reason, the job runs unchanged
with its original configuration (same graceful fallback as the plugin):

  - ``RABBIT_API_KEY``              required to optimize; absent -> skip
  - ``RABBIT_API_BASE_URL``         optional base URL override
  - ``RABBIT_DEFAULT_PRICING_MODE`` ``on_demand`` (default) or ``slot_based``
  - ``RABBIT_RESERVATION_IDS``      comma-separated reservation IDs; empty -> skip
"""

from __future__ import annotations

import logging
import os

from google.cloud import bigquery
from rabbit_bq_job_optimizer import OptimizationConfig, RabbitBQJobOptimizer

logger = logging.getLogger(__name__)

VALID_PRICING_MODES = ("on_demand", "slot_based")


class RabbitBigQueryClient(bigquery.Client):
    """A ``bigquery.Client`` whose ``query()`` optimizes pricing via Rabbit."""

    def query(self, query, job_config=None, **kwargs):  # type: ignore[override]
        job_config = self._optimize(query, job_config)
        return super().query(query, job_config=job_config, **kwargs)

    def _optimize(self, query, job_config):
        """Route the job configuration through Rabbit, falling back on failure."""
        try:
            api_key = (os.environ.get("RABBIT_API_KEY") or "").strip()
            if not api_key:
                logger.warning(
                    "Rabbit BQ Optimizer: RABBIT_API_KEY is not set. "
                    "Proceeding with original job configuration."
                )
                return job_config

            reservation_ids = [
                rid.strip()
                for rid in (os.environ.get("RABBIT_RESERVATION_IDS") or "").split(",")
                if rid.strip()
            ]
            if not reservation_ids:
                logger.warning(
                    "Rabbit BQ Optimizer: RABBIT_RESERVATION_IDS is empty. "
                    "Proceeding with original job configuration."
                )
                return job_config

            pricing_mode = (
                os.environ.get("RABBIT_DEFAULT_PRICING_MODE") or "on_demand"
            ).strip()
            if pricing_mode not in VALID_PRICING_MODES:
                logger.warning(
                    "Rabbit BQ Optimizer: Invalid RABBIT_DEFAULT_PRICING_MODE '%s'. "
                    "Must be one of: %s. Proceeding with original job configuration.",
                    pricing_mode,
                    ", ".join(VALID_PRICING_MODES),
                )
                return job_config

            # A QueryJobConfig carries no SQL text, so inject it to build the
            # configuration dict the SDK expects.
            cfg = job_config.to_api_repr() if job_config is not None else {}
            cfg.setdefault("query", {})
            cfg["query"]["query"] = query
            cfg["query"].setdefault("useLegacySql", False)

            client_kwargs = {"api_key": api_key}
            base_url = (os.environ.get("RABBIT_API_BASE_URL") or "").strip()
            if base_url:
                client_kwargs["base_url"] = base_url

            opt_client = RabbitBQJobOptimizer(**client_kwargs)
            optimization = OptimizationConfig(
                type="reservation_assignment",
                config={
                    "defaultPricingMode": pricing_mode,
                    "reservationIds": reservation_ids,
                },
            )
            result = opt_client.optimize_job(
                configuration={"configuration": cfg},
                enabledOptimizations=[optimization],
            )
            logger.info("Rabbit BQ Optimizer: Received optimization result: %s", result)

            optimized_query = result.optimizedJob["configuration"]["query"]
            # The SQL text is passed separately to super().query(); it is not part
            # of a QueryJobConfig.
            optimized_query.pop("query", None)
            return bigquery.QueryJobConfig.from_api_repr(optimized_query)
        except Exception as exc:  # noqa: BLE001 - mirror the plugin's broad fallback
            logger.warning(
                "Rabbit BQ Optimizer: Optimization failed due to error: %s. "
                "Proceeding with original job configuration.",
                exc,
            )
            return job_config
