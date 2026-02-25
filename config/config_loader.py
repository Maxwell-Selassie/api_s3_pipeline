import yaml
import logging 
from pathlib import Path 
from dotenv import load_dotenv
import os 

load_dotenv() 

logger = logging.getLogger(__name__)

# resolve path relative to this file's location
config_path = Path(__file__).parent / "config.yaml"

def load_yaml() -> dict:
    """
    Loads and parses the YAML config file.
    Called once at module import time
    Fails immediately if file is missing or malformed
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: ",config_path
        )
    
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if config is None:
        raise ValueError(f"Config file is empty")
    
    logger.debug(f"Config loaded from ",config_path)
    return config 

config = load_yaml()

# Public constants
cities = config["weather_api"]["cities"]
api_base_url = config["weather_api"]["api_base_url"]
hourly_variables = config["weather_api"]["request_params"]["hourly_variables"]
s3_bucket = config["s3"]["bucket_name"]
raw_folder = config["s3"]["folders"][0]
processed_folder = config["s3"]["folders"][1]
partition_format = config["s3"]["partition_format"]

# Credentials from .env
aws_access_key_id = os.getenv("Access_key_ID")
aws_secret_access_key = os.getenv("Secret_access_key")
aws_region = os.getenv("AWS_REGION")

def validate_config() -> None: 
    """
    Validates that all required config values are present and sensible.
    Called explicitly from main.py before the pipeline starts.
    
    Raises ValueError immediately if anything is wrong.
    """
    errors = []

    if not cities:
        errors.append("No cities defined in config")

    if len(cities) == 0:
        errors.append("Cities list is empty")

    for city in cities:
        required_keys = {"name","lat","lon","timezone"}
        missing_keys = required_keys - set(city.keys())

        if missing_keys:
            errors.append(
                "City ",city.get("name","unknown"), "missing required keys: ", missing_keys
            )

        if not (-180 <= city.get("lat", 0) <= 90):
            errors.append(
                f"City ", city["name"], "has invalid latitude: ",city["lat"]
            )

        if not (-180 <= city.get("lon", 0) <= 180):
            errors.append(
                f"City ",city["name"], "has invalid latitude: ",city["lon"]
            )

    if not hourly_variables:
        errors.append(
            "No hourly variables defined"
        )

    if not s3_bucket:
        errors.append("S3 bucket name is missing")

    if not aws_access_key_id:
        errors.append(f"aws_access_key_id not found in environment")

    if not aws_secret_access_key:
        errors.append(f"aws_secret_access_key not found in environment")

    if errors:
        raise ValueError(
            f"Config validation failed with {len(errors)} error(s):\n"
            + "\n".join(f" - {e}" for e in errors)
        )
    
    logger.info(
        f"Config validated - "
        f"{len(cities)} cities, "
        f"{len(hourly_variables)} variables"
    )

if __name__ == "__main__":
    validate_config()
    print(f"Cities: ",[c["name"] for c in cities])
    print(f"Variables : {hourly_variables}")
    print(f"Bucket: {s3_bucket}")