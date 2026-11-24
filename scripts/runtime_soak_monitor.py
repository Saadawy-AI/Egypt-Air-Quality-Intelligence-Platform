import csv, ctypes, datetime, json, pathlib, subprocess, time
ROOT = pathlib.Path(r"D:\\Projects\\egypt_air_quality\\Egypt-Air-Quality-Intelligence-Platform")
OUT = ROOT / "artifacts" / "runtime_soak_2026-11-24.csv"
OUT.parent.mkdir(exist_ok=True)
def run(args):
    p = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=35, shell=False)
    return p.stdout.strip(), p.stderr.strip(), p.returncode
def available_gib():
    class M(ctypes.Structure):
        _fields_=[("dwLength",ctypes.c_ulong),("dwMemoryLoad",ctypes.c_ulong),("ullTotalPhys",ctypes.c_ulonglong),("ullAvailPhys",ctypes.c_ulonglong),("ullTotalPageFile",ctypes.c_ulonglong),("ullAvailPageFile",ctypes.c_ulonglong),("ullTotalVirtual",ctypes.c_ulonglong),("ullAvailVirtual",ctypes.c_ulonglong),("ullAvailExtendedVirtual",ctypes.c_ulonglong)]
    m=M(); m.dwLength=ctypes.sizeof(M)
    return (m.ullAvailPhys/(1024**3),m.dwMemoryLoad) if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)) else (None,None)
def sql(q):
    o,e,c=run(["docker","compose","exec","-T","postgres","psql","-U","postgres","-d","airquality","-Atc",q])
    return o if c==0 else "ERROR:"+e[:200]
def filecount(service,path):
    o,e,c=run(["docker","compose","exec","-T",service,"sh","-lc",f"find {path} -type f -name '*.parquet' | wc -l"])
    return o if c==0 else "ERROR:"+e[:120]
def inspect():
    o,e,c=run(["docker","inspect","-f","{{.Name}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.Status}}","egypt-air-quality-spark-bronze-1","egypt-air-quality-spark-silver-1","egypt-air-quality-spark-gold-1","egypt-air-quality-mqtt-kafka-bridge-1"])
    return o.replace("\n",";") if c==0 else "ERROR:"+e[:150]
def logs(service):
    o,e,c=run(["docker","compose","logs","--since","65s","--no-color",service])
    s=(o+" "+e).lower()
    return {"bronze_overrun":("batch duration" in s and "30" in s) or "trigger" in s and "exceed" in s,
            "silver_errors":("error" in s or "exception" in s),
            "gold_errors":("error" in s or "exception" in s),
            "bridge_blocking":("event loop blocking" in s),
            "bridge_delivery_fail":("delivery failed" in s or "queue full" in s)}
baseline = {"gold_batches":sql("SELECT count(*) FROM processed_gold_batches"),"gold_latest":sql("SELECT coalesce(max(processed_at)::text,'') FROM processed_gold_batches"),"fact_rows":sql("SELECT count(*) FROM fact_hourly_measurements")}
fields=["timestamp","available_ram_gib","ram_used_pct","bronze_parquet","silver_parquet","gold_parquet","gold_batch_count","gold_latest_processed_at","fact_rows","fact_latest_updated_at","container_states","bronze_overrun","silver_errors","gold_errors","bridge_blocking","bridge_delivery_fail"]
with OUT.open("w",newline="",encoding="utf-8") as f:
    wr=csv.DictWriter(f,fieldnames=fields); wr.writeheader()
    for i in range(31):
        avail,used=available_gib()
        rec={"timestamp":datetime.datetime.now().astimezone().isoformat(timespec="seconds"),"available_ram_gib":round(avail,3) if avail is not None else None,"ram_used_pct":used,
        "bronze_parquet":filecount("spark-bronze","/opt/project-data/bronze"),"silver_parquet":filecount("spark-silver","/opt/project-data/silver"),"gold_parquet":filecount("spark-gold","/opt/project-data/gold"),
        "gold_batch_count":sql("SELECT count(*) FROM processed_gold_batches"),"gold_latest_processed_at":sql("SELECT coalesce(max(processed_at)::text,'') FROM processed_gold_batches"),
        "fact_rows":sql("SELECT count(*) FROM fact_hourly_measurements"),"fact_latest_updated_at":sql("SELECT coalesce(max(updated_at)::text,'') FROM fact_hourly_measurements"),"container_states":inspect()}
        for service in ["spark-bronze","spark-silver","spark-gold","mqtt-kafka-bridge"]:
            x=logs(service)
            if service=="spark-bronze": rec["bronze_overrun"]=x["bronze_overrun"]
            if service=="spark-silver": rec["silver_errors"]=x["silver_errors"]
            if service=="spark-gold": rec["gold_errors"]=x["gold_errors"]
            if service=="mqtt-kafka-bridge": rec["bridge_blocking"]=x["bridge_blocking"]; rec["bridge_delivery_fail"]=x["bridge_delivery_fail"]
        wr.writerow(rec); f.flush()
        print(json.dumps(rec,ensure_ascii=False),flush=True)
        if i<30: time.sleep(60)
print("SOAK_COMPLETE",json.dumps(baseline),str(OUT),flush=True)
