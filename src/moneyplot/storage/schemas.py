"""DuckDB table schemas."""

import duckdb


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create all analytical tables if they don't exist."""

    con.execute("""
        CREATE TABLE IF NOT EXISTS mutations (
            id_mutation         VARCHAR,
            date_mutation       DATE,
            nature_mutation     VARCHAR,
            valeur_fonciere     DOUBLE,
            code_departement    VARCHAR,
            code_commune        VARCHAR,
            nom_commune         VARCHAR,
            code_postal         VARCHAR,
            id_parcelle         VARCHAR,
            type_local          VARCHAR,
            surface_reelle_bati DOUBLE,
            nombre_pieces       INTEGER,
            surface_terrain     DOUBLE,
            longitude           DOUBLE,
            latitude            DOUBLE,
            prix_m2             DOUBLE,
            annee               INTEGER,
            trimestre           INTEGER
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS indices_prix (
            date        DATE,
            indice      DOUBLE,
            type_bien   VARCHAR,
            zone        VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS taux_hypothecaires (
            date    DATE,
            taux    DOUBLE,
            source  VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS communes (
            code_commune    VARCHAR PRIMARY KEY,
            nom_commune     VARCHAR,
            code_departement VARCHAR,
            code_region     VARCHAR,
            population      INTEGER,
            revenu_median   DOUBLE,
            latitude        DOUBLE,
            longitude       DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dpe (
            id_dpe              VARCHAR,
            code_commune        VARCHAR,
            id_parcelle         VARCHAR,
            classe_energie      VARCHAR,
            classe_ges          VARCHAR,
            annee_construction  INTEGER,
            surface_habitable   DOUBLE,
            date_etablissement  DATE
        )
    """)
