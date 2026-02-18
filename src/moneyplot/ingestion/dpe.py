"""Fetch DPE (Diagnostic de Performance Énergétique) data from ADEME API."""

import logging

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

ADEME_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"


def fetch_dpe_for_commune(code_commune: str, limit: int = 10000) -> pd.DataFrame:
    """Fetch DPE records for a single commune.

    Returns a DataFrame with columns matching the dpe table schema.
    """
    logger.info("Fetching DPE for commune %s", code_commune)

    rows = []
    offset = 0
    page_size = min(limit, 1000)

    while offset < limit:
        resp = httpx.get(
            ADEME_API_URL,
            params={
                "q_fields": "code_insee_commune_actualise",
                "q": code_commune,
                "size": page_size,
                "after": offset,
                "select": (
                    "identifiant_dpe,"
                    "code_insee_commune_actualise,"
                    "identifiant_ban,"
                    "classe_consommation_energie,"
                    "classe_estimation_ges,"
                    "annee_construction,"
                    "surface_habitable_logement,"
                    "date_etablissement_dpe"
                ),
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        for r in results:
            rows.append({
                "id_dpe": r.get("identifiant_dpe"),
                "code_commune": r.get("code_insee_commune_actualise"),
                "id_parcelle": None,  # Not directly available, needs geocoding
                "classe_energie": r.get("classe_consommation_energie"),
                "classe_ges": r.get("classe_estimation_ges"),
                "annee_construction": _safe_int(r.get("annee_construction")),
                "surface_habitable": _safe_float(r.get("surface_habitable_logement")),
                "date_etablissement": r.get("date_etablissement_dpe"),
            })

        offset += len(results)
        if len(results) < page_size:
            break

    df = pd.DataFrame(rows)
    if not df.empty and "date_etablissement" in df.columns:
        df["date_etablissement"] = pd.to_datetime(df["date_etablissement"], errors="coerce")
    logger.info("Fetched %d DPE records for commune %s", len(df), code_commune)
    return df


def fetch_dpe_for_department(code_dept: str, limit_per_commune: int = 10000) -> pd.DataFrame:
    """Fetch DPE data for all communes in a department.

    This queries by department code prefix.
    """
    logger.info("Fetching DPE for department %s", code_dept)

    rows = []
    offset = 0
    page_size = 1000
    total_limit = 100_000

    while offset < total_limit:
        resp = httpx.get(
            ADEME_API_URL,
            params={
                "qs": f"code_insee_commune_actualise:{code_dept}*",
                "size": page_size,
                "after": offset,
                "select": (
                    "identifiant_dpe,"
                    "code_insee_commune_actualise,"
                    "classe_consommation_energie,"
                    "classe_estimation_ges,"
                    "annee_construction,"
                    "surface_habitable_logement,"
                    "date_etablissement_dpe"
                ),
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        for r in results:
            rows.append({
                "id_dpe": r.get("identifiant_dpe"),
                "code_commune": r.get("code_insee_commune_actualise"),
                "id_parcelle": None,
                "classe_energie": r.get("classe_consommation_energie"),
                "classe_ges": r.get("classe_estimation_ges"),
                "annee_construction": _safe_int(r.get("annee_construction")),
                "surface_habitable": _safe_float(r.get("surface_habitable_logement")),
                "date_etablissement": r.get("date_etablissement_dpe"),
            })

        offset += len(results)
        if len(results) < page_size:
            break

    df = pd.DataFrame(rows)
    if not df.empty and "date_etablissement" in df.columns:
        df["date_etablissement"] = pd.to_datetime(df["date_etablissement"], errors="coerce")
    logger.info("Fetched %d DPE records for department %s", len(df), code_dept)
    return df


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None
