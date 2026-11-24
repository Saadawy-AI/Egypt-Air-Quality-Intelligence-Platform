"""Read-only inventory of OpenAQ Bronze, Silver and quarantine partitions."""
from pathlib import Path
from collections import Counter
import json
import pyarrow.parquet as pq

ROOT = Path("/opt/project-data")

def audit(layer):
    root = ROOT / layer / "openaq_measurements"
    if not root.exists():
        return []
    result = []
    for partition in sorted(p for p in root.iterdir() if p.is_dir()):
        files = sorted(partition.rglob("*.parquet"))
        rows = 0
        ids = []
        sources = set()
        errors = Counter()
        first = last = None
        schemas = set()
        topics = Counter()
        for file in files:
            pf = pq.ParquetFile(file)
            rows += pf.metadata.num_rows
            schemas.update(pf.schema_arrow.names)
            wanted = [c for c in ("record_id", "source", "event_time", "topic", "validation_errors") if c in pf.schema_arrow.names]
            if wanted:
                data = pq.read_table(file, columns=wanted).to_pydict()
                topics.update(str(x) for x in data.get("topic", []) if x is not None)
                ids.extend(x for x in data.get("record_id", []) if x is not None)
                sources.update(x for x in data.get("source", []) if x)
                errors.update(x for x in data.get("validation_errors", []) if x)
                times = [x for x in data.get("event_time", []) if x is not None]
                if times:
                    lo, hi = min(times), max(times)
                    first = lo if first is None or lo < first else first
                    last = hi if last is None or hi > last else last
        item = {"partition": partition.name, "files": len(files), "rows": rows, "schema": sorted(schemas), "topics": dict(topics)}
        if layer == "silver":
            item.update({"distinct_record_ids": len(set(ids)), "duplicate_record_ids": len(ids)-len(set(ids)), "sources": sorted(sources), "first_event_time": str(first), "last_event_time": str(last)})
        if layer == "quarantine":
            item["validation_errors"] = dict(errors)
        result.append(item)
    return result

print(json.dumps({"bronze": audit("bronze"), "silver": audit("silver"), "quarantine": audit("quarantine")}, indent=2, default=str))
