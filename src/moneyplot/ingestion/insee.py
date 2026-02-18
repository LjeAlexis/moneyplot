"""Fetch Notaires-INSEE price indices via INSEE BDM API (SDMX-ML)."""

import logging
from xml.etree import ElementTree as ET

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# Notaires-INSEE price index series
# 010567006 = France entière - Appartements
# 010567007 = France entière - Maisons
# 010567008 = Île-de-France - Appartements
# 010567009 = Province - Appartements
SERIES_IDS = {
    "010567006": ("Appartements", "France"),
    "010567007": ("Maisons", "France"),
    "010567008": ("Appartements", "Île-de-France"),
    "010567009": ("Appartements", "Province"),
}

INSEE_BDM_URL = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM"

# SDMX 2.1 namespaces used by INSEE
NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "gen": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


def fetch_price_indices() -> pd.DataFrame:
    """Fetch quarterly price indices from INSEE BDM.

    The API returns SDMX-ML (XML) regardless of Accept header.
    Returns a DataFrame with columns: date, indice, type_bien, zone.
    """
    all_rows = []

    for series_id, (type_bien, zone) in SERIES_IDS.items():
        url = f"{INSEE_BDM_URL}/{series_id}"
        logger.info("Fetching INSEE series %s (%s, %s)", series_id, type_bien, zone)

        try:
            resp = httpx.get(url, timeout=30)
            resp.raise_for_status()

            rows = _parse_sdmx_xml(resp.text, type_bien, zone)
            all_rows.extend(rows)
            logger.info("Series %s: %d observations", series_id, len(rows))
        except Exception:
            logger.exception("Failed to fetch series %s", series_id)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    logger.info("Fetched %d price index data points", len(df))
    return df


def _parse_sdmx_xml(xml_text: str, type_bien: str, zone: str) -> list[dict]:
    """Parse SDMX-ML StructureSpecificData or GenericData from INSEE."""
    root = ET.fromstring(xml_text)
    rows = []

    # Try StructureSpecificData format first (most common from INSEE)
    # In this format, observations are Obs elements with TIME_PERIOD and OBS_VALUE attributes
    for obs in root.iter():
        tag = obs.tag.split("}")[-1] if "}" in obs.tag else obs.tag
        if tag == "Obs":
            period = obs.attrib.get("TIME_PERIOD")
            value = obs.attrib.get("OBS_VALUE")
            if period and value:
                date = _quarter_to_date(period)
                if date:
                    rows.append({
                        "date": date,
                        "indice": float(value),
                        "type_bien": type_bien,
                        "zone": zone,
                    })

    if rows:
        return rows

    # Fallback: try GenericData format
    for obs in root.iter(f"{{{NS['gen']}}}Obs"):
        period = None
        value = None
        for dim in obs.iter(f"{{{NS['gen']}}}ObsDimension"):
            period = dim.attrib.get("value")
        for val in obs.iter(f"{{{NS['gen']}}}ObsValue"):
            value = val.attrib.get("value")
        if period and value:
            date = _quarter_to_date(period)
            if date:
                rows.append({
                    "date": date,
                    "indice": float(value),
                    "type_bien": type_bien,
                    "zone": zone,
                })

    return rows


def _quarter_to_date(period: str) -> str | None:
    """Convert '2023-Q1' or '2023-T1' to '2023-01-01'."""
    try:
        year, q = period.replace("T", "Q").split("-Q")
        month = (int(q) - 1) * 3 + 1
        return f"{year}-{month:02d}-01"
    except (ValueError, IndexError):
        return None
