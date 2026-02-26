import logging
import sys 
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path 
import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))  # add src/ to path for imports

from config.config_loader import validate_config
from src.ingest import fetch_all_cities
from src.storage import write_processed, write_raw, read_raw
from src.transform import transform_all

def setup_logging(run_id: str) -> None: 
    """
    Configures structured logging with run ID on every line
    Writes to both the console and a dated log file
    """
    log_dir = Path("logs/")
    log_dir.mkdir(parents=True, exist_ok=True) 

    log_format = (
        f"%(asctime)s | run_id={run_id} | "
        f"%(levelname)s | %(name)s | %(message)s"
    )

    logging.basicConfig(
        level=logging.INFO, 
        format=log_format,
        handlers=[
            logging.FileHandler(log_dir/f"pipeline_{run_id}.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_pipeline(target_date: date = None):
    """
    Orchestrates the complete weather ingestion pipeline
    
        Stage order:
        0. Config validation   — fail fast if misconfigured
        1. Ingest              — fetch all cities from API
        2. Write raw           — store JSON responses in S3
        3. Read raw            — retrieve from S3 for transform
        4. Transform           — flatten and enrich
        5. Write processed     — store CSVs in S3

    Args:
        target_date: date to process. Defaults to yesterday UTC.
        Accepts explicit date for manual backfills.
    """
    run_id = str(uuid.uuid4())[:8]
    setup_logging(run_id=run_id)

    logger = logging.getLogger(__name__)

    if target_date is None:
        target_date = datetime.now(timezone.utc) - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")
    start_time = datetime.now(timezone.utc)

    logger.info("="*60)
    logger.info(f"PIPELINE STARTING")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Target date: {date_str}")
    logger.info("="*60)

    # stage 0: config validation
    try:
        validate_config()
    except ValueError as e:
        logger.error(f"Config validation failed: {e}")
        sys.exit(1)

    # stage 1: Ingestion
    successful_ingestions, failed_ingestions = fetch_all_cities(
        target_date=target_date
    )

    if not successful_ingestions:
        logger.error(f"All cities failed ingestion - pipeline halted")
        sys.exit(1)

    # stage 2: write raw to s3
    raw_written = []
    raw_failed = []

    for ingestion in successful_ingestions:
        city_name = ingestion["city_name"]
        try:
            key = write_raw(
                city_name, target_date, ingestion["raw_response"]
            )
            raw_written.append(city_name)

        except Exception as e:
            logger.error(f"Raw write failed for {city_name}: {e}")
            raw_failed.append(city_name)

    # stage 3 and 4: read_raw + Transform
    # read back from s3 and transform in one loop
    # only process cities that were successfully written
    to_transform = []

    for city_name in raw_written:
        try:
            raw_data = read_raw(city_name, target_date)
            to_transform.append({
                "city_name": city_name,
                "raw_response": raw_data
            })
        except Exception as e:
            logger.error(f"Raw read failed for {city_name} : {e}")
            raw_failed.append(city_name)

    transformed, transformed_failed = transform_all(
        to_transform, date_str
    )

    # stage 5: write processed CSVs
    processed_written = []
    processed_failed = []
    total_rows = 0

    for city_name, df in transformed:
        try:
            write_processed(city_name, target_date, df)
            processed_written.append(city_name)
            total_rows += len(df)

        except Exception as e:
            logger.error(
                f"Processed write failed for {city_name} : {e}"
            )
            processed_failed.append(city_name)

    # pipeline summary
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    all_failed = list(set(
        failed_ingestions + raw_failed +
        transformed_failed + processed_failed
    ))

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Run ID            : {run_id}")
    logger.info(f"  Target date       : {date_str}")
    logger.info(f"  Duration          : {duration:.2f}s")
    logger.info(f"  Cities successful : {len(processed_written)}/5")
    logger.info(f"  Cities failed     : {all_failed if all_failed else 'none'}")
    logger.info(f"  Total rows loaded : {total_rows} (expected: {len(processed_written) * 24})")
    logger.info(
        f"  Dataset location  : "
        f"s3://weather-pipeline-raw-maxwell-selassie/"
        f"processed/year={target_date.year}/"
        f"month={target_date.strftime('%m')}/"
        f"day={target_date.strftime('%d')}/"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
    