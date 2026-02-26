import boto3
import json
import logging
import io
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv


from config.config_loader import (
    s3_bucket as S3_BUCKET,
    aws_region as AWS_REGION,
    raw_folder as RAW_FOLDER,
    processed_folder as PROCESSED_FOLDER,
    partition_format as PARTITION_FORMAT,
    aws_access_key_id as AWS_ACCESS_KEY_ID,
    aws_secret_access_key as AWS_SECRET_ACCESS_KEY
)

load_dotenv()
logger = logging.getLogger(__name__)


def _get_s3_client():
    """
    Creates and returns a boto3 S3 client.

    Why a function instead of a module-level client?
    Module-level clients are created at import time. If credentials
    are missing at import, the error message is confusing — it appears
    to come from the import, not from the actual S3 operation.
    Creating the client at call time means credential errors surface
    exactly where they're relevant.

    Why not cache the client?
    For a pipeline running once per day, connection overhead is
    negligible. Simplicity outweighs the marginal performance gain.
    At high frequency you'd cache using functools.lru_cache.
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


def _build_s3_key(
    folder: str,
    city_name: str,
    date: datetime.date,
    extension: str
) -> str:
    """
    Builds the full S3 object key from its components.

    Why private?
    Path construction is an internal concern. No module outside
    storage.py should know or care about the partition structure.
    If partitioning changes from daily to hourly, only this
    function changes — nothing outside storage.py is affected.

    Example output:
        raw/year=2024/month=01/day=15/london.json
        processed/year=2024/month=01/day=15/london.csv
    """
    partition = PARTITION_FORMAT.format(
        year=date.strftime("%Y"),
        month=date.strftime("%m"),
        day=date.strftime("%d")
    )

    return f"{folder}/{partition}/{city_name}.{extension}"


def write_raw(
    city_name: str,
    date: datetime.date,
    raw_response: dict
) -> str:
    """
    Serializes a Python dict to JSON and writes it to the raw S3 folder.

    Returns the S3 key so the caller can log exactly where
    the file was written — useful for debugging and auditing.

    Why overwrite if file exists?
    S3 last-write-wins is our idempotency strategy. Running the
    pipeline twice on the same day produces identical files.
    No special handling needed — S3 handles this naturally.
    """
    s3_key = _build_s3_key(RAW_FOLDER, city_name, date, "json")

    # dict → JSON string → bytes
    # indent=2 makes the stored JSON human-readable
    # This costs slightly more storage but saves significant
    # debugging time when you need to inspect raw files manually
    json_bytes = json.dumps(raw_response, indent=2).encode("utf-8")

    s3 = _get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_bytes,
        ContentType="application/json"
    )

    logger.info(f"Raw JSON written → s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def read_raw(
    city_name: str,
    date: datetime.date
) -> dict:
    """
    Reads a raw JSON file from S3 and returns a parsed Python dict.

    Why return dict not bytes?
    Every caller wants to work with Python objects, not raw bytes.
    Deserializing here means callers never deal with JSON parsing —
    they just receive usable data. Single responsibility applied
    at the function level.

    Raises:
        KeyError: if the file doesn't exist in S3
        json.JSONDecodeError: if the file exists but isn't valid JSON
    """
    s3_key = _build_s3_key(RAW_FOLDER, city_name, date, "json")

    s3 = _get_s3_client()

    logger.info(f"Reading raw JSON ← s3://{S3_BUCKET}/{s3_key}")

    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)

    # response["Body"] is a streaming object — read() gets the bytes
    raw_bytes = response["Body"].read()

    # bytes → JSON string → Python dict
    return json.loads(raw_bytes.decode("utf-8"))


def write_processed(
    city_name: str,
    date: datetime.date,
    df: pd.DataFrame
) -> str:
    """
    Writes a normalized DataFrame to the processed S3 folder as CSV.

    Why use io.StringIO instead of writing to disk first?
    Writing to disk requires managing temporary files — creating them,
    cleaning them up, handling permission errors. StringIO is an
    in-memory file buffer. The CSV never touches disk — it goes
    directly from DataFrame to S3. Cleaner, faster, no cleanup needed.

    Returns the S3 key for logging and auditing.
    """
    s3_key = _build_s3_key(PROCESSED_FOLDER, city_name, date, "csv")

    # DataFrame → CSV string in memory (no disk write)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # StringIO position is at end after write — reset to beginning
    # before reading, otherwise you get an empty string
    csv_buffer.seek(0)

    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    s3 = _get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv"
    )

    logger.info(f"Processed CSV written → s3://{S3_BUCKET}/{s3_key}")
    return s3_key
