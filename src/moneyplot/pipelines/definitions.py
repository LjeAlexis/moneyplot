"""Dagster definitions entry point for Moneyplot."""

from dagster import Definitions

from moneyplot.pipelines.assets import (
    cleaned_dvf,
    dvf_in_duckdb,
    mortgage_rates,
    price_indices,
    raw_dvf,
)
from moneyplot.pipelines.resources import DuckDBResource
from moneyplot.pipelines.schedules import dvf_monthly, macro_quarterly

defs = Definitions(
    assets=[raw_dvf, cleaned_dvf, dvf_in_duckdb, price_indices, mortgage_rates],
    resources={"duckdb_resource": DuckDBResource()},
    schedules=[dvf_monthly, macro_quarterly],
)
