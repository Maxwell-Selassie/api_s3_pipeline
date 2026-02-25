import boto3
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_s3():
    s3 = boto3.client(
        "s3",
        region_name = os.getenv("AWS_REGION")
    )
    response = s3.list_buckets()

    buckets = [b["Name"] for b in response["Buckets"]]
    print(f"S3 connection OK - buckets: {buckets}")

def test_api():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=5.6037&longitude=-0.1870"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        "&timezone=Africa%2FAccra&forecast_days=1"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    print(f"API connection OK - keys: ",list(data.keys()))
    print(f"Hourly variables: ",list(data["hourly"].keys()))
    print(f"Hours of data: ",len(data["hourly"]["time"]))

if __name__ == "__main__":
    test_s3()
    test_api()