from pathlib import Path
from collections import Counter
import json
import pyarrow.dataset as ds

root=Path('/opt/project-data')
bronze=root/'bronze/openaq_measurements/ingestion_date=2026-11-23'
silver=root/'silver/openaq_measurements'

def load(path, cols):
 d=ds.dataset(str(path),format='parquet')
 return d.to_table(columns=[c for c in cols if c in d.schema.names]).to_pylist()

b=load(bronze,['kafka_key','raw_value','partition','offset','kafka_timestamp','ingestion_timestamp'])
h=load(silver/'batch_id=historical_backfill_v1',['record_id','source','timestamp','metadata'])
stream=[]
for p in sorted(silver.glob('batch_id=*')):
 if p.name.endswith('historical_backfill_v1'): continue
 fs=list(p.rglob('*.parquet'))
 if fs: stream += load(p,['record_id','source','timestamp','metadata','kafka_key','partition','offset'])

# derive the exact Silver record identity from raw OpenAQ payloads
bronze_ids=[]
bronze_rows=[]
for row in b:
 try: raw=json.loads(row['raw_value'])
 except Exception: continue
 meta=raw.get('metadata') or {}; data=raw.get('data') or {}
 rid=':'.join(str(x) for x in ['openaq',meta.get('sensor_id'),data.get('pollutant'),raw.get('timestamp')])
 bronze_ids.append(rid)
 bronze_rows.append({'record_id':rid,'kafka_key':row.get('kafka_key'),'partition':row.get('partition'),'offset':row.get('offset'),'kafka_timestamp':str(row.get('kafka_timestamp')),'ingestion_timestamp':str(row.get('ingestion_timestamp')),'ingested_at':raw.get('ingested_at'),'pollutant':data.get('pollutant'),'timestamp':raw.get('timestamp')})

hist_ids={x['record_id'] for x in h if x.get('record_id')}
stream_ids={x['record_id'] for x in stream if x.get('record_id')}
bronze_counts=Counter(bronze_ids)
unique_bronze=set(bronze_ids)
summary={
 'bronze_rows':len(b),'bronze_unique_record_ids':len(unique_bronze),'bronze_duplicate_record_id_rows':sum(n-1 for n in bronze_counts.values()),
 'bronze_record_ids_in_historical_silver':len(unique_bronze & hist_ids),
 'bronze_record_ids_in_streaming_silver':len(unique_bronze & stream_ids),
 'bronze_record_ids_in_neither_silver':len(unique_bronze-hist_ids-stream_ids),
 'historical_silver_rows':len(h),'streaming_silver_rows':len(stream),'streaming_unique_record_ids':len(stream_ids),
 'unmatched_bronze_examples':[x for x in bronze_rows if x['record_id'] not in hist_ids and x['record_id'] not in stream_ids][:20],
 'bronze_duplicate_examples':[{'record_id':k,'count':n} for k,n in bronze_counts.items() if n>1][:20],
}
# Count quarantine files/rows in the 2026-11-23 date and corresponding batch directories.
qroot=root/'quarantine/openaq_measurements'
q=[]
if qroot.exists():
 for p in sorted(qroot.glob('batch_id=*')):
  files=list(p.rglob('*.parquet'))
  if files:
   d=ds.dataset(str(p),format='parquet'); q.append({'batch':p.name,'rows':d.count_rows(),'files':len(files)})
summary['quarantine_nonempty_batches']=q
# Gold outputs may use Hive window_date subdirectories for legacy batches.
gold=root/'gold/hourly_measurements_v4'; gd=[]
for p in sorted(gold.glob('batch_id=*')):
 files=list(p.rglob('*.parquet'))
 if files:
  d=ds.dataset(str(p),format='parquet'); gd.append({'batch':p.name,'rows':d.count_rows(),'parquet_files':len(files),'root_flat_files':len(list(p.glob('*.parquet')))})
summary['gold_nonempty_batches']=gd
print(json.dumps(summary,indent=2,default=str))
