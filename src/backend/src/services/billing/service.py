"""Estimate from the same per-call token telemetry used by the execution trace.

No request/response payloads are returned. Missing usage/rates are counted as
unpriced, never inferred from text lengths and never represented as free calls.
"""

from collections import defaultdict
from decimal import Decimal

from sqlalchemy.exc import IntegrityError

from src.core.exceptions import ConflictError, ForbiddenError
from src.core.permissions import check_role_in_context
from src.repositories.model_billing_rate_repository import ModelBillingRateRepository
from src.schemas.billing import BillingSummary, UsageBreakdown, UsageTotals
from src.services.trace.usage import TraceUsageService


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
        return result if result >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _usage(row):
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    metadata = (
        row.get("trace_metadata") if isinstance(row.get("trace_metadata"), dict) else {}
    )
    data = {**extra, **metadata}
    usage = data.get("usage") if isinstance(data.get("usage"), dict) else data
    input_ = _number(usage.get("prompt_tokens", usage.get("input_tokens")))
    output = _number(usage.get("completion_tokens", usage.get("output_tokens")))
    cached = (
        _number(usage.get("cached_prompt_tokens", usage.get("cache_read_input_tokens")))
        or 0
    )
    total = _number(usage.get("total_tokens"))
    if total is None and input_ is not None and output is not None:
        total = input_ + output
    return str(data.get("model") or "Unknown model"), input_, output, cached, total


class _Totals:
    def __init__(self):
        self.values = UsageTotals()
        self.cost = Decimal(0)

    def add(self, input_, output, cached, total, cost):
        value = self.values
        value.calls += 1
        value.measured_calls += int(total is not None)
        value.input_tokens += input_ or 0
        value.output_tokens += output or 0
        value.cached_input_tokens += min(cached, input_ or 0)
        value.total_tokens += total or 0
        if cost is not None:
            value.priced_calls += 1
            self.cost += cost
            value.estimated_cost_usd = float(self.cost)


class BillingService:
    def __init__(self, session):
        self.rates = ModelBillingRateRepository(session)
        self.usage = TraceUsageService(session)

    @staticmethod
    def _authorize(context, *, manage=False):
        allowed = ["admin"] if manage else ["admin", "editor", "operator"]
        if (
            not context
            or not context.primary_group_id
            or not check_role_in_context(context, allowed)
        ):
            raise ForbiddenError("You do not have access to billing in this teamspace.")

    async def save_rate(self, context, rate):
        self._authorize(context, manage=True)
        try:
            return await self.rates.put(context.primary_group_id, rate.model_dump())
        except IntegrityError as error:
            raise ConflictError(
                "This model rate changed. Refresh and try again."
            ) from error

    async def summary(self, context, query):
        self._authorize(context)
        rates = await self.rates.for_group(context.primary_group_id)
        by_model = {rate.model: rate for rate in rates}
        totals = _Totals()
        models, runs, days = (defaultdict(_Totals) for _ in range(3))
        names, types, seen = {}, {}, set()
        # DB timestamps are naive UTC throughout the existing trace writer.
        async for row in self.usage.iter_calls(
            context,
            query.start.replace(tzinfo=None),
            query.end.replace(tzinfo=None),
            query.execution_ids,
        ):
            # Retransmitted spans represent the same call; distinct calls with
            # identical token counts must still count separately.
            key = (row["execution_id"], row.get("span_id") or row["id"])
            if key in seen:
                continue
            seen.add(key)
            model, input_, output, cached, total = _usage(row)
            rate = by_model.get(model)
            cost = None
            if rate and input_ is not None and output is not None:
                cache_rate = rate.cached_input_per_million
                # A missing cache rate cannot silently charge discounted tokens
                # at the normal input rate.
                if not cached or cache_rate is not None:
                    cache = min(cached, input_)
                    cost = (
                        Decimal(input_ - cache) * rate.input_per_million
                        + Decimal(output) * rate.output_per_million
                        + Decimal(cache) * (cache_rate or Decimal(0))
                    ) / Decimal(1000000)
            run = row["execution_id"]
            day = row["created_at"].date().isoformat()
            names[run] = row.get("run_name") or run
            types[run] = row.get("execution_type")
            for item in (totals, models[model], runs[run], days[day]):
                item.add(input_, output, cached, total, cost)

        def breakdown(items, *, run=False):
            return [
                UsageBreakdown(
                    id=key,
                    label=names.get(key, key) if run else key,
                    execution_type=types.get(key) if run else None,
                    **value.values.model_dump(),
                )
                for key, value in items
            ]

        rank = lambda item: (item[1].cost, item[1].values.total_tokens)
        return BillingSummary(
            start=query.start,
            end=query.end,
            totals=totals.values,
            models=breakdown(sorted(models.items(), key=rank, reverse=True)),
            runs=breakdown(sorted(runs.items(), key=rank, reverse=True), run=True),
            days=breakdown(sorted(days.items())),
            rates=rates,
            can_manage_rates=check_role_in_context(context, ["admin"]),
        )
