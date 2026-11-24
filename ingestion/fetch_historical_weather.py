"""Fetch research-only historical weather reanalysis; never publishes to Kafka/Gold."""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ingestion.config.governorates import GOVERNORATES

API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
)


def fetch_weather(governorate, start_date, end_date, session=None, model="best_match"):
    if governorate not in GOVERNORATES:
        raise ValueError(f"Unsupported governorate: {governorate}")
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end < start:
        raise ValueError("end_date must not precede start_date")
    location = GOVERNORATES[governorate]
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
        "wind_speed_unit": "ms",
        "models": model,
    }
    client = session or requests.Session()
    last_error = None
    for attempt in range(3):
        try:
            response = client.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            hourly = payload.get("hourly") or {}
            times = hourly.get("time") or []
            if not times:
                raise RuntimeError("Historical weather API returned no hourly timestamps.")
            frame = pd.DataFrame({"window_start": pd.to_datetime(times, utc=True)})
            missing = []
            for variable in HOURLY_VARIABLES:
                values = hourly.get(variable)
                if values is None or len(values) != len(times):
                    missing.append(variable)
                else:
                    frame[variable] = pd.to_numeric(values, errors="coerce")
            if missing:
                raise RuntimeError("Missing or misaligned weather variables: " + ", ".join(missing))
            frame["governorate"] = governorate
            frame = frame.sort_values("window_start").drop_duplicates(
                ["governorate", "window_start"], keep="last"
            )
            if frame["window_start"].isna().any():
                raise RuntimeError("Historical weather response contains invalid timestamps.")
            metadata = {
                "provider": "Open-Meteo Historical Weather API",
                "endpoint": API_URL,
                "dataset": f"{model} historical reanalysis",
                "license": "CC BY 4.0",
                "attribution": "Weather data by Open-Meteo.com",
                "governorate": governorate,
                "latitude": payload.get("latitude"),
                "longitude": payload.get("longitude"),
                "elevation_m": payload.get("elevation"),
                "timezone": payload.get("timezone"),
                "start_date": start_date,
                "end_date": end_date,
                "rows": int(len(frame)),
                "hourly_units": payload.get("hourly_units", {}),
                "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                "usage": "Research-only exogenous features; not station observations.",
            }
            return frame, metadata
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Historical weather retrieval failed: {last_error}") from last_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--governorate", default="Cairo")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--model", default="best_match", choices=("best_match", "era5", "era5_land", "ecmwf_ifs"))
    args = parser.parse_args()
    frame, metadata = fetch_weather(
        args.governorate, args.start_date, args.end_date, model=args.model
    )
    metadata["null_counts"] = frame[list(HOURLY_VARIABLES)].isna().sum().to_dict()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = args.output.with_suffix(".json")
    csv_tmp = args.output.with_name(args.output.name + ".tmp")
    json_tmp = metadata_path.with_name(metadata_path.name + ".tmp")
    frame.to_csv(csv_tmp, index=False)
    json_tmp.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    csv_tmp.replace(args.output)
    json_tmp.replace(metadata_path)
    print(json.dumps({
        "csv_path": str(args.output),
        "metadata_path": str(metadata_path),
        "rows": len(frame),
        "first_timestamp": frame["window_start"].min().isoformat(),
        "last_timestamp": frame["window_start"].max().isoformat(),
        "metadata": metadata,
    }, indent=2))


if __name__ == "__main__":
    main()
