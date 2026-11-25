"""Read-only overlap check for recent OpenAQ streaming batch vs historical Silver."""
from pathlib import Path
import json
import pyarrow.parquet as pq

ROOT = Path("/opt/project-data/silver/openaq_measurements")
recent_dir = ROOT / "batch_id=00000000000000000012"
historical_file = ROOT / "batch_id=historical_backfill_v1" / "part-00000.parquet"
recent_files = list(recent_dir.rglob("*.parquet"))
recent_tables = [pq.read_table(p, columns=["record_id", "event_time", "data", "metadata", "topic", "partition", "offset"]) for p in recent_files]
recent = __import__("pyarrow").concat_tables(recent_tables).to_pydict()
historical = pq.read_table(historical_file, columns=["record_id"]).column("record_id").to_pylist()
historical_ids = set(historical)
recent_ids = recent["record_id"]
overlap = [x for x in recent_ids if x in historical_ids]
print(json.dumps({
    "recent_batch_rows": len(recent_ids),
    "recent_distinct_ids": len(set(recent_ids)),
    "historical_rows": len(historical),
    "overlap_with_historical": len(overlap),
    "new_vs_historical": len(set(recent_ids) - historical_ids),
    "event_time_min": str(min(recent["event_time"])),
    "event_time_max": str(max(recent["event_time"])),
    "topics": sorted(set(recent["topic"])),
    "partition_offsets": sorted(set((p, o) for p, o in zip(recent["partition"], recent["offset"]))),
}, indent=2, default=str))
