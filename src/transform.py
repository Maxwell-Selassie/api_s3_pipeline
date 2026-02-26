import pandas as pd
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _extract_units(raw_response: dict) -> dict:
    """
    Extracts the hourly_units from the API response.
    Returns a dict mapping variable name to unit string.

    Example: {"temperature_2m": "°C", "wind_speed_10m": "km/h"}
    """
    return raw_response.get("hourly_units", {})


def _build_unit_suffix(unit: str) -> str:
    """
    Converts a unit string to a clean column name suffix.

    Why clean it?
    Column names with special characters like ° or % cause
    issues in SQL, some CSV parsers, and downstream tools.
    Clean suffixes are safe everywhere.

    Examples:
        "°C"    → "_c"
        "%"     → "_pct"
        "km/h"  → "_kmh"
        "m"     → "_m"
        "mm"    → "_mm"
        "°"     → "_deg"
    """
    unit_map = {
        "°C": "_c",
        "%": "_pct",
        "km/h": "_kmh",
        "m": "_m",
        "mm": "_mm",
        "°": "_deg",
        "iso8601": "",      # time column needs no suffix
    }
    return unit_map.get(unit, "")


def _flatten_hourly(raw_response: dict) -> pd.DataFrame:
    """
    Converts the API's parallel arrays into a flat DataFrame.

    The API returns:
        {"time": [t1, t2, ...], "temperature_2m": [v1, v2, ...], ...}

    pd.DataFrame() aligns parallel arrays by index automatically,
    producing one row per hour — exactly what we need.

    This is the core JSON normalization operation.
    """
    hourly = raw_response["hourly"]

    # pd.DataFrame on a dict of equal-length lists
    # produces rows automatically — no zip, no loop needed
    df = pd.DataFrame(hourly)

    return df


def _rename_columns_with_units(
    df: pd.DataFrame,
    units: dict
) -> pd.DataFrame:
    """
    Renames columns to include unit suffixes for self-documentation.

    Example:
        temperature_2m → temperature_2m_c
        relative_humidity_2m → relative_humidity_2m_pct
        wind_speed_10m → wind_speed_10m_kmh

    "time" is excluded — it gets renamed to "timestamp" instead.
    """
    rename_map = {}

    for col in df.columns:
        if col == "time":
            rename_map[col] = "timestamp"
            continue

        unit = units.get(col, "")
        suffix = _build_unit_suffix(unit)
        rename_map[col] = f"{col}{suffix}"

    return df.rename(columns=rename_map)


def transform(
    city_name: str,
    date: str,
    raw_response: dict
) -> pd.DataFrame:
    """
    Transforms a raw API response dict into a flat, enriched DataFrame.

    Steps:
        1. Flatten parallel hourly arrays into rows
        2. Rename columns with unit suffixes
        3. Parse timestamp column to datetime
        4. Add city_name, date, ingested_at columns
        5. Reorder columns for readability

    Args:
        city_name: name of the city (becomes a column)
        date: date string YYYY-MM-DD (becomes a column)
        raw_response: complete API response dict from read_raw

    Returns:
        DataFrame with 24 rows — one per hour of the day
    """
    logger.info(f"Transforming {city_name} for {date}")

    # Step 1 — flatten
    units = _extract_units(raw_response)
    df = _flatten_hourly(raw_response)

    # Step 2 — rename with unit suffixes
    df = _rename_columns_with_units(df, units)

    # Step 3 — parse timestamp
    # Convert ISO8601 string to proper datetime object
    # errors="coerce" turns unparseable values to NaT (not crash)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Step 4 — add metadata columns
    # These columns don't come from the API — we add them
    df["city_name"] = city_name
    df["date"] = date
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Step 5 — reorder for readability
    # Metadata columns first, then weather variables
    metadata_cols = ["city_name", "date", "timestamp", "ingested_at"]
    weather_cols = [c for c in df.columns if c not in metadata_cols]
    df = df[metadata_cols + weather_cols]

    logger.info(
        f"Transform complete — "
        f"shape: {df.shape}, "
        f"columns: {list(df.columns)}"
    )

    return df


def transform_all(
    successful_ingestions: list,
    date: str
) -> tuple[list, list]:
    """
    Transforms all successfully ingested city responses.

    Mirrors the fault isolation pattern from fetch_all_cities —
    one city failing transformation never stops others.

    Args:
        successful_ingestions: list of dicts from fetch_all_cities
        date: date string YYYY-MM-DD

    Returns:
        - transformed: list of (city_name, DataFrame) tuples
        - failed: list of city names that failed transformation
    """
    transformed = []
    failed = []

    for ingestion in successful_ingestions:
        city_name = ingestion["city_name"]
        raw_response = ingestion["raw_response"]

        try:
            df = transform(city_name, date, raw_response)
            transformed.append((city_name, df))

        except Exception as e:
            logger.error(
                f"Transform failed for {city_name}: "
                f"{type(e).__name__}: {e}"
            )
            failed.append(city_name)
            continue

    logger.info(
        f"Transform complete — "
        f"successful: {len(transformed)}, "
        f"failed: {failed if failed else 'none'}"
    )

    return transformed, failed

