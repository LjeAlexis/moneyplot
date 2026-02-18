"""Clean and transform raw DVF data."""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "dvf"
PROCESSED_DIR = Path(__file__).resolve().parents[3] / "data" / "processed"

# Property types we care about
TYPES_LOCAL = {"Maison", "Appartement"}


def clean_dvf(raw_dir: Path | None = None, output_dir: Path | None = None) -> Path:
    """Read raw DVF CSVs, clean, and write a single Parquet file.

    Steps:
    1. Read all department CSVs into DuckDB (in-memory)
    2. Filter to sales (Vente) of houses and apartments
    3. Deduplicate by id_mutation (keep one row per mutation with aggregated surfaces)
    4. Compute prix/m²
    5. Write to Parquet
    """
    raw = raw_dir or RAW_DIR
    out = (output_dir or PROCESSED_DIR) / "dvf_clean.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    csv_pattern = str(raw / "dvf_*.csv.gz")
    logger.info("Reading DVF CSVs from %s", csv_pattern)

    con = duckdb.connect()

    # Read all CSVs at once via glob — DuckDB handles gzip natively
    con.execute(f"""
        CREATE TABLE raw_dvf AS
        SELECT * FROM read_csv('{csv_pattern}',
            auto_detect=true,
            ignore_errors=true,
            header=true
        )
    """)

    row_count = con.execute("SELECT count(*) FROM raw_dvf").fetchone()[0]
    logger.info("Loaded %d raw rows", row_count)

    # Clean: filter to Vente, relevant property types, deduplicate mutations
    con.execute("""
        CREATE TABLE cleaned AS
        SELECT
            id_mutation,
            date_mutation,
            nature_mutation,
            valeur_fonciere,
            code_departement,
            code_commune,
            nom_commune,
            code_postal,
            id_parcelle,
            type_local,
            surface_reelle_bati,
            nombre_pieces_principales AS nombre_pieces,
            surface_terrain,
            longitude,
            latitude,
            -- Compute prix/m²
            CASE
                WHEN surface_reelle_bati > 0 THEN valeur_fonciere / surface_reelle_bati
                ELSE NULL
            END AS prix_m2,
            YEAR(date_mutation) AS annee,
            QUARTER(date_mutation) AS trimestre
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id_mutation, type_local
                    ORDER BY surface_reelle_bati DESC NULLS LAST
                ) AS rn
            FROM raw_dvf
            WHERE nature_mutation = 'Vente'
              AND type_local IN ('Maison', 'Appartement')
              AND valeur_fonciere > 0
              AND valeur_fonciere < 10000000
        )
        WHERE rn = 1
    """)

    clean_count = con.execute("SELECT count(*) FROM cleaned").fetchone()[0]
    logger.info("Cleaned dataset: %d rows", clean_count)

    # Write to Parquet
    con.execute(f"COPY cleaned TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    logger.info("Written to %s", out)

    con.close()
    return out


def load_parquet_to_duckdb(
    parquet_path: Path, target_con: duckdb.DuckDBPyConnection
) -> int:
    """Load the cleaned Parquet file into the persistent DuckDB mutations table."""
    target_con.execute("DELETE FROM mutations")
    target_con.execute(f"""
        INSERT INTO mutations
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    count = target_con.execute("SELECT count(*) FROM mutations").fetchone()[0]
    logger.info("Loaded %d rows into mutations table", count)
    return count
