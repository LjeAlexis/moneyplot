"""Download DVF (Demandes de Valeurs Foncières) data from Etalab."""

import logging
from pathlib import Path

import httpx
from tqdm import tqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"
RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "dvf"

# Available years on Etalab geo-dvf
YEARS = ["2020", "2021", "2022", "2023", "2024", "2025"]

# Departments excluded from DVF: Alsace (67, 68), Moselle (57), Mayotte (976)
EXCLUDED_DEPTS = {"20", "57", "67", "68", "976"}

ALL_DEPTS = (
    [f"{i:02d}" for i in range(1, 96) if f"{i:02d}" not in EXCLUDED_DEPTS]
    + ["2A", "2B"]
    + [f"{i}" for i in range(971, 975)]  # DOM: 971-974 (not 976)
)


def download_department_year(
    dept: str, year: str, output_dir: Path | None = None
) -> Path:
    """Download the DVF CSV for a single department and year.

    URL pattern: {BASE_URL}/{year}/departements/{dept}.csv.gz
    Returns the path to the downloaded file.
    """
    out = (output_dir or RAW_DIR) / f"dvf_{year}_{dept}.csv.gz"
    out.parent.mkdir(parents=True, exist_ok=True)

    url = f"{BASE_URL}/{year}/departements/{dept}.csv.gz"
    logger.info("Downloading DVF %s dept %s from %s", year, dept, url)

    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(out, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=f"DVF {year}/{dept}", disable=total == 0
        ) as pbar:
            for chunk in resp.iter_bytes(chunk_size=65_536):
                f.write(chunk)
                pbar.update(len(chunk))

    logger.info("Saved %s (%d bytes)", out, out.stat().st_size)
    return out


def download_all(
    departments: list[str] | None = None,
    years: list[str] | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Download DVF CSVs for all (or selected) departments and years."""
    depts = departments or ALL_DEPTS
    yrs = years or YEARS
    paths = []
    for year in yrs:
        for dept in depts:
            try:
                p = download_department_year(dept, year, output_dir)
                paths.append(p)
            except httpx.HTTPStatusError as exc:
                logger.warning("Failed to download dept %s year %s: %s", dept, year, exc)
    return paths
