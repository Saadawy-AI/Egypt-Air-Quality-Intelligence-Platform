import json
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.compute as pc
import psycopg

ROOT = Path("/opt/project-data")

def map_dict(value):
    if isinstance(value, dict):
        return value
    return dict(value or [])

MESSAGE_ID = "wokwi-validation-04"
SILVER_BATCH = 69
GOLD_BATCH = 1_000_000_069

# 1) Locate the original MQTT/Kafka payload in Bronze and retain Kafka coordinates.
bronze_ds = ds.dataset(str(ROOT / "bronze" / "air_quality"), format="parquet", partitioning="hive")
bronze = bronze_ds.to_table(
    columns=["raw_value", "topic", "partition", "offset", "kafka_key", "kafka_timestamp", "ingestion_timestamp"]
)
mask = pc.match_substring(bronze["raw_value"], pattern=MESSAGE_ID)
matches = bronze.filter(mask)
if matches.num_rows != 1:
    raise SystemExit(f"Expected exactly one Bronze match for {MESSAGE_ID}, found {matches.num_rows}")
b = matches.to_pylist()[0]
payload = json.loads(b["raw_value"])
metadata = payload.get("metadata") or {}
device_id = metadata.get("device_id")
if not device_id:
    raise SystemExit("Bronze payload has no metadata.device_id")
print("BRONZE", json.dumps({
    "message_id": MESSAGE_ID, "source": payload.get("source"),
    "device_id": device_id, "topic": b["topic"], "partition": b["partition"],
    "offset": b["offset"], "kafka_key": b["kafka_key"],
    "timestamp": payload.get("timestamp"), "metadata": metadata,
}, default=str))

# 2) Verify the same Kafka coordinates and device identity survived Silver.
silver_path = ROOT / "silver" / "air_quality" / f"batch_id={SILVER_BATCH:020d}"
silver = ds.dataset(str(silver_path), format="parquet").to_table()
silver_rows = [
    row for row in silver.to_pylist()
    if row.get("source") == "wokwi_esp32"
    and map_dict(row.get("metadata")).get("device_id") == device_id
    and row.get("partition") == b["partition"]
    and row.get("offset") == b["offset"]
]
if len(silver_rows) != 1:
    raise SystemExit(f"Expected exactly one Silver match by device_id+Kafka coordinates, found {len(silver_rows)}")
s = silver_rows[0]
print("SILVER", json.dumps({
    "silver_batch": SILVER_BATCH, "record_id": s.get("record_id"),
    "source": s.get("source"), "device_id": map_dict(s.get("metadata")).get("device_id"),
    "topic": s.get("topic"), "partition": s.get("partition"), "offset": s.get("offset"),
    "event_time": s.get("event_time"), "governorate": s.get("governorate"),
    "latitude": s.get("latitude"), "longitude": s.get("longitude"),
    "data": s.get("data"), "metadata": s.get("metadata"),
}, default=str))

# 3) Read the exact Gold contribution produced from Silver batch 69.
gold_path = ROOT / "gold" / "hourly_measurements_v4" / f"batch_id={GOLD_BATCH:020d}"
gold = ds.dataset(str(gold_path), format="parquet").to_table()
gold_rows = [r for r in gold.to_pylist() if r.get("source") == "wokwi_esp32"]
print("GOLD", json.dumps({
    "gold_batch": GOLD_BATCH, "silver_batch": SILVER_BATCH,
    "rows_for_wokwi": len(gold_rows), "rows": gold_rows,
}, default=str))
if not gold_rows:
    raise SystemExit("No Wokwi contribution found in the Gold batch")

# 4) Reconcile each exact Gold aggregate row with the serving fact table.
conninfo = psycopg.conninfo.make_conninfo(
    host="172.29.0.10", port="5432", dbname="airquality",
    user="airquality", password=__import__("os").environ["PGPASSWORD"],
)
with psycopg.connect(conninfo) as conn:
    ledger = conn.execute(
        "SELECT batch_id,row_count,source_path FROM processed_gold_batches WHERE batch_id=%s",
        (GOLD_BATCH,),
    ).fetchone()
    if not ledger:
        raise SystemExit("Gold batch missing from PostgreSQL processed batch ledger")
    results = []
    for g in gold_rows:
        row = conn.execute("""
            SELECT f.window_start, f.average_value, f.minimum_value, f.maximum_value,
                   f.measurement_count, f.latitude, f.longitude,
                   s.source_name, gov.governorate_name, m.metric_name, m.unit
            FROM fact_hourly_measurements f
            JOIN dim_source s USING(source_key)
            JOIN dim_governorate gov USING(governorate_key)
            JOIN dim_metric m USING(metric_key)
            WHERE s.source_name=%s AND gov.governorate_name=%s
              AND m.metric_name=%s AND m.unit=%s AND f.window_start=%s
        """, (
            g["source"], g["governorate"], g["metric"], g.get("unit") or "",
            g["window_start"],
        )).fetchone()
        exact = bool(row) and (
            abs(row[1] - float(g["average_value"])) < 1e-9
            and abs(row[2] - float(g["minimum_value"])) < 1e-9
            and abs(row[3] - float(g["maximum_value"])) < 1e-9
            and row[4] == int(g["measurement_count"])
        )
        results.append({
            "metric": g["metric"], "gold_value": g["average_value"],
            "gold_count": g["measurement_count"], "postgres_match": bool(row),
            "postgres_average": row[1] if row else None,
            "postgres_count": row[4] if row else None,
            "exact_aggregate_match": exact,
        })
    print("POSTGRES", json.dumps({
        "ledger": {"batch_id": ledger[0], "row_count": ledger[1], "source_path": ledger[2]},
        "reconciled_metrics": results,
        "all_exact": bool(results) and all(x["exact_aggregate_match"] for x in results),
    }, default=str))
    if not results or not all(x["exact_aggregate_match"] for x in results):
        raise SystemExit("PostgreSQL aggregate reconciliation failed")
print("RESULT PASS: exact Bronze message -> Silver Kafka offset/device -> Gold aggregate -> PostgreSQL fact values")
