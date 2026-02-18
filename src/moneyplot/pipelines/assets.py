"""Dagster asset definitions for Moneyplot."""

import logging
from pathlib import Path

from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset

from moneyplot.ingestion.dvf import ALL_DEPTS, download_all
from moneyplot.ingestion.ecb import fetch_mortgage_rates
from moneyplot.ingestion.insee import fetch_price_indices
from moneyplot.pipelines.resources import DuckDBResource
from moneyplot.transform.dvf_clean import clean_dvf, load_parquet_to_duckdb

logger = logging.getLogger(__name__)


class DVFConfig(Config):
    """Configuration for DVF download."""

    departments: list[str] = []  # empty = all


# ── DVF Assets ───────────────────────────────────────────────────────────────


@asset(group_name="dvf")
def raw_dvf(context: AssetExecutionContext, config: DVFConfig) -> MaterializeResult:
    """Download raw DVF CSV files from Etalab."""
    depts = config.departments or None
    paths = download_all(departments=depts)
    return MaterializeResult(
        metadata={
            "num_files": MetadataValue.int(len(paths)),
            "departments": MetadataValue.text(", ".join(p.stem.split("_")[1] for p in paths)),
        }
    )


@asset(deps=[raw_dvf], group_name="dvf")
def cleaned_dvf(context: AssetExecutionContext) -> MaterializeResult:
    """Clean raw DVF data and produce a Parquet file."""
    parquet_path = clean_dvf()
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    return MaterializeResult(
        metadata={
            "parquet_path": MetadataValue.path(str(parquet_path)),
            "size_mb": MetadataValue.float(round(size_mb, 1)),
        }
    )


@asset(deps=[cleaned_dvf], group_name="dvf")
def dvf_in_duckdb(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> MaterializeResult:
    """Load cleaned DVF Parquet into DuckDB."""
    processed = Path(__file__).resolve().parents[3] / "data" / "processed" / "dvf_clean.parquet"
    con = duckdb_resource.get_connection()
    count = load_parquet_to_duckdb(processed, con)
    con.close()
    return MaterializeResult(
        metadata={"row_count": MetadataValue.int(count)},
    )


# ── Macro Assets ─────────────────────────────────────────────────────────────


@asset(group_name="macro")
def price_indices(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> MaterializeResult:
    """Fetch Notaires-INSEE price indices and load into DuckDB."""
    df = fetch_price_indices()
    con = duckdb_resource.get_connection()
    con.execute("DELETE FROM indices_prix")
    con.execute("INSERT INTO indices_prix SELECT * FROM df")
    count = con.execute("SELECT count(*) FROM indices_prix").fetchone()[0]
    con.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


@asset(group_name="macro")
def mortgage_rates(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> MaterializeResult:
    """Fetch ECB mortgage rates and load into DuckDB."""
    df = fetch_mortgage_rates()
    con = duckdb_resource.get_connection()
    con.execute("DELETE FROM taux_hypothecaires")
    con.execute("INSERT INTO taux_hypothecaires SELECT * FROM df")
    count = con.execute("SELECT count(*) FROM taux_hypothecaires").fetchone()[0]
    con.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})
