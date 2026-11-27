from pathlib import Path
import json
import pyarrow.dataset as ds
import psycopg
from postgres.loaders.gold_loader import connection_info

root = Path('/opt/project-data')
bronze = root / 'bronze/openaq_measurements/ingestion_date=2026-11-23'
silver = root / 'silver/openaq_measurements'
gold = root / 'gold/hourly_measurements_v4'

# Summarize Bronze by topic/partition, distinct keys, and key multiplicity.
d = ds.dataset(str(bronze), format='parquet')
t = d.to_table(columns=['kafka_key','topic','partition','offset','raw_value'])
v = t.to_pydict()
from collections import Counter
keys = [x for x in v['kafka_key'] if x is not None]
key_counts = Counter(keys)
bronze_summary = {
 'rows': t.num_rows,
 'distinct_keys': len(key_counts),
 'duplicate_key_rows': sum(n-1 for n in key_counts.values()),
 'topic_counts': dict(Counter(v['topic'])),
 'partition_counts': dict(Counter(v['partition'])),
 'offset_ranges': {},
}
for part in sorted(set(v['partition'])):
 vals = [o for p,o in zip(v['partition'],v['offset']) if p == part and o is not None]
 bronze_summary['offset_ranges'][str(part)] = [min(vals),max(vals)] if vals else None

# Compare Silver streaming batches to Bronze by Kafka key; collect all non-empty batch dirs.
silver_batches=[]
for p in sorted(silver.glob('batch_id=*')):
 if p.name.endswith('historical_backfill_v1'):
  continue
 files=list(p.rglob('*.parquet'))
 if not files: continue
 sd=ds.dataset(str(p),format='parquet')
 cols=[c for c in ['kafka_key','record_id','source','event_time','timestamp','offset'] if c in sd.schema.names]
 st=sd.to_table(columns=cols).to_pydict()
 ks=[x for x in st.get('kafka_key',[]) if x]
 silver_batches.append({'batch':p.name,'rows':len(ks),'distinct_keys':len(set(ks)),'bronze_key_matches':sum(1 for x in ks if x in key_counts),'sample_keys':ks[:3]})

# Gold directory and database ledger.
gold_dirs=[]
for p in sorted(gold.glob('batch_id=*')):
 files=list(p.glob('*.parquet'))
 if files or (p/'_SUCCESS').exists(): gold_dirs.append({'batch':p.name,'parquet_files':len(files),'success':(p/'_SUCCESS').exists()})
with psycopg.connect(connection_info()) as conn:
 ledger=conn.execute('select batch_id,row_count,processed_at from processed_gold_batches order by batch_id').fetchall()

print(json.dumps({'bronze':bronze_summary,'silver_nonempty_batches':silver_batches,'gold_batch_dirs':gold_dirs,'postgres_gold_ledger':[{'batch_id':r[0],'row_count':r[1],'processed_at':str(r[2])} for r in ledger]},indent=2,default=str))
