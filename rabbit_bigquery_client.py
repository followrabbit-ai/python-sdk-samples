"""A drop-in ``bigquery.Client`` whose ``query()`` is optimized by Rabbit.

``RabbitBigQueryClient`` subclasses :class:`google.cloud.bigquery.Client` and
overrides :meth:`query` so call sites are byte-for-byte the BigQuery API, but
every query job is transparently routed through the Rabbit Dynamic Pricing
optimizer first. This mirrors what the upstream Airflow plugin does by patching
``BigQueryHook.insert_job``.

Configuration is split to match how the SDK works:

- ``api_key`` / ``base_url`` are constructor arguments forwarded directly to
  :class:`rabbit_bq_job_optimizer.RabbitBQJobOptimizer`. Leaving them as ``None``
  preserves the SDK's own precedence: explicit argument -> ``RABBIT_API_KEY`` /
  ``RABBIT_API_BASE_URL`` environment variable -> default base URL.
- ``optimization_config`` is a plain, mutable dict holding the optimization
  parameters the SDK does *not* read from the environment. Provide it in the
  constructor or omit it, and update it any time afterward:

    - ``default_pricing_mode``: ``"on_demand"`` (default) or ``"slot_based"``
    - ``reservation_ids``: list of reservation IDs (empty -> skip optimization)

If the optimization config is missing/invalid, or the optimizer call fails for
any reason, the job runs unchanged with its original configuration (the same
graceful fallback as the plugin).
"""

from __future__ import annotations

import logging

from google.cloud import bigquery
from rabbit_bq_job_optimizer import OptimizationConfig, RabbitBQJobOptimizer

logger = logging.getLogger(__name__)

VALID_PRICING_MODES = ("on_demand", "slot_based")


class RabbitBigQueryClient(bigquery.Client):
    """A ``bigquery.Client`` whose ``query()`` optimizes pricing via Rabbit."""

    def __init__(
        self,
        *args,
        api_key: str | None = None,
        base_url: str | None = None,
        optimization_config: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        # Forwarded to the SDK; None lets the SDK read env vars / use its default.
        self._api_key = api_key
        self._base_url = base_url
        # Public and mutable: set here or update later, e.g.
        #   client.optimization_config["reservation_ids"] = [...]
        self.optimization_config: dict = (
            dict(optimization_config) if optimization_config else {}
        )

    def query(self, query, job_config=None, **kwargs):  # type: ignore[override]
        job_config = self._optimize(query, job_config)
        return super().query(query, job_config=job_config, **kwargs)

    def _optimize(self, query, job_config):
        """Route the job configuration through Rabbit, falling back on failure."""
        try:
            config = self.optimization_config or {}

            reservation_ids = config.get("reservation_ids") or []
            if not reservation_ids:
                logger.warning(
                    "Rabbit BQ Optimizer: no reservation_ids configured. "
                    "Proceeding with original job configuration."
                )
                return job_config

            pricing_mode = config.get("default_pricing_mode", "on_demand")
            if pricing_mode not in VALID_PRICING_MODES:
                logger.warning(
                    "Rabbit BQ Optimizer: invalid default_pricing_mode '%s'. "
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

            optimizer = RabbitBQJobOptimizer(
                api_key=self._api_key, base_url=self._base_url
            )
            optimization = OptimizationConfig(
                type="reservation_assignment",
                config={
                    "defaultPricingMode": pricing_mode,
                    "reservationIds": list(reservation_ids),
                },
            )
            result = optimizer.optimize_job(
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
