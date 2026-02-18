"""Enrichment transforms — join DVF with DPE and geographic data."""

import logging

import duckdb

logger = logging.getLogger(__name__)


def enrich_mutations_with_dpe(con: duckdb.DuckDBPyConnection) -> int:
    """Create a view joining mutations with DPE data on code_commune.

    This is a best-effort join since DPE records don't always have parcel IDs.
    We join on commune + approximate surface match.
    """
    con.execute("""
        CREATE OR REPLACE VIEW mutations_enriched AS
        SELECT
            m.*,
            d.classe_energie,
            d.classe_ges,
            d.annee_construction
        FROM mutations m
        LEFT JOIN (
            SELECT DISTINCT ON (code_commune, surface_habitable)
                code_commune,
                surface_habitable,
                classe_energie,
                classe_ges,
                annee_construction
            FROM dpe
            WHERE classe_energie IS NOT NULL
            ORDER BY code_commune, surface_habitable, date_etablissement DESC
        ) d
        ON m.code_commune = d.code_commune
           AND ABS(m.surface_reelle_bati - d.surface_habitable) < 5
    """)

    count = con.execute("SELECT count(*) FROM mutations_enriched WHERE classe_energie IS NOT NULL").fetchone()[0]
    logger.info("Enriched mutations: %d rows with DPE data", count)
    return count
