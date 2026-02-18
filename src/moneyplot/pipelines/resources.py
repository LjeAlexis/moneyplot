"""Dagster resources (shared DuckDB connection)."""

from pathlib import Path

import duckdb
from dagster import ConfigurableResource, InitResourceContext

from moneyplot.storage.db import DEFAULT_DB_PATH
from moneyplot.storage.schemas import create_tables


class DuckDBResource(ConfigurableResource):
    """A Dagster resource that provides a DuckDB connection."""

    db_path: str = str(DEFAULT_DB_PATH)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        path = Path(self.db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(path))
        create_tables(con)
        return con
