"""Fetch mortgage interest rates from the ECB Statistical Data Warehouse."""

import logging

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ECB series for French mortgage rates (new business, house purchase, over 5 years)
ECB_SERIES_KEY = "M.FR.B.A2C.A.C.A.2250.EUR.N"
ECB_API_URL = f"https://data-api.ecb.europa.eu/service/data/MIR/{ECB_SERIES_KEY}"


def fetch_mortgage_rates() -> pd.DataFrame:
    """Fetch monthly French mortgage rates from ECB.

    Returns a DataFrame with columns: date, taux, source.
    """
    logger.info("Fetching ECB mortgage rates: %s", ECB_SERIES_KEY)

    resp = httpx.get(
        ECB_API_URL,
        params={"format": "csvdata"},
        headers={"Accept": "text/csv"},
        timeout=30,
    )
    resp.raise_for_status()

    # Parse the CSV response
    from io import StringIO

    df_raw = pd.read_csv(StringIO(resp.text))

    # ECB CSV has TIME_PERIOD and OBS_VALUE columns
    if "TIME_PERIOD" not in df_raw.columns or "OBS_VALUE" not in df_raw.columns:
        logger.warning("Unexpected ECB response format: %s", df_raw.columns.tolist())
        return pd.DataFrame(columns=["date", "taux", "source"])

    df = pd.DataFrame({
        "date": pd.to_datetime(df_raw["TIME_PERIOD"]),
        "taux": pd.to_numeric(df_raw["OBS_VALUE"], errors="coerce"),
        "source": "ECB",
    })
    df = df.dropna(subset=["taux"])

    logger.info("Fetched %d mortgage rate data points", len(df))
    return df
