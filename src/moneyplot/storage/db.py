"""DuckDB connection manager."""

from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "moneyplot.duckdb"


def get_connection(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. Creates the file and parent dirs if needed."""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))
