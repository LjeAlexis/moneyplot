"""Dagster schedules for Moneyplot."""

from dagster import ScheduleDefinition

from moneyplot.pipelines.assets import dvf_in_duckdb, mortgage_rates, price_indices

# DVF is updated semestrially (April + October) — run monthly to catch updates
dvf_monthly = ScheduleDefinition(
    name="dvf_monthly",
    target=[dvf_in_duckdb],
    cron_schedule="0 3 1 * *",  # 1st of each month at 3am
)

# Macro data — quarterly
macro_quarterly = ScheduleDefinition(
    name="macro_quarterly",
    target=[price_indices, mortgage_rates],
    cron_schedule="0 4 1 1,4,7,10 *",  # 1st of Jan/Apr/Jul/Oct at 4am
)
