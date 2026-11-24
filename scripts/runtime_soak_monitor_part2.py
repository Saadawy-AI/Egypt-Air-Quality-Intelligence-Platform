import csv, ctypes, datetime, json, pathlib, subprocess, time
ROOT = pathlib.Path(r"D:\\Projects\\egypt_air_quality\\Egypt-Air-Quality-Intelligence-Platform")
OUT = ROOT / "artifacts" / "runtime_soak_2026-11-24_part2.csv"
OUT.parent.mkdir(exist_ok=True)
def run(args, timeout=30):
    p = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout, shell=False)
    return p.stdout.strip(), p.stderr.strip(), p.returncode
def ram():
    class M(ctypes.Structure):
        _fields_=[("dwLength",ctypes.c_ulong),("dwMemoryLoad",ctypes.c_ulong),("ullTotalPhys",ctypes.c_ulonglong),("ullAvailPhys",ctypes.c_ulonglong),("ullTotalPageFile",ctypes.c_ulonglong),("ullAvailPageFile",ctypes.c_ulonglong),("ullTotalVirtual",ctypes.c_ulonglong),("ullAvailVirtual",ctypes.c_ulonglong),("ullAvailExtendedVirtual",ctypes.c_ulonglong)]
    m=M(); m.dwLength=ctypes.sizeof(M)
    return (round(m.ullAvailPhys/(1024**3),3),m.dwMemoryLoad) if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)) else (None,None)
def sql(q):
    o,e,c=run(["docker","compose","exec","-T","postgres","psql","-U","postgres","-d","airquality","-Atc",q])
    return o if c==0 else "ERROR:"+e[:180]
def count_files(service,path):
    o,e,c=run(["docker","compose","exec","-T",service,"sh","-lc",f"find {path} -type f -name '*.parquet' | wc -l"])
    return o if c==0 else "ERROR:"+e[:100]
def checkpoint(service,path):
    o,e,c=run(["docker","compose","exec","-T",service,"sh","-lc",f"ls -1 {path}/offsets 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -1"])
    return o if c==0 else "ERR"
def inspect():
    names=["egypt-air-quality-spark-bronze-1","egypt-air-quality-spark-silver-1","egypt-air-quality-spark-gold-1","egypt-air-quality-mqtt-kafka-bridge-1"]
    o,e,c=run(["docker","inspect","-f","{{.Name}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.Status}}",*names])
    return ";".join(o.splitlines()) if c==0 else "ERROR:"+e[:150]
def log_lines(service):
    o,e,c=run(["docker","compose","logs","--since","65s","--no-color",service])
    return o.splitlines() if c==0 else [("LOG_ERROR "+e[:160])]
fields=["timestamp","available_ram_gib","ram_used_pct","bronze_parquet","silver_parquet","gold_parquet","bronze_offsets","silver_offsets","gold_offsets","silver_nonempty_batch_logs","silver_notifications","gold_batch_write_logs","postgres_load_logs","gold_ledger","fact_rows_latest","container_states","bronze_trigger_overrun","silver_errors","gold_errors","bridge_blocking","bridge_delivery_fail"]
with OUT.open("w",newline="",encoding="utf-8") as f:
    wr=csv.DictWriter(f,fieldnames=fields); wr.writeheader()
    for i in range(21):
        avail,used=ram()
        logs={s:log_lines(s) for s in ["spark-bronze","spark-silver","spark-gold","postgres-loader","mqtt-kafka-bridge"]}
        rec={"timestamp":datetime.datetime.now().astimezone().isoformat(timespec="seconds"),"available_ram_gib":avail,"ram_used_pct":used,
        "bronze_parquet":count_files("spark-bronze","/opt/project-data/bronze"),"silver_parquet":count_files("spark-silver","/opt/project-data/silver"),"gold_parquet":count_files("spark-gold","/opt/project-data/gold/hourly_measurements_v4"),
        "bronze_offsets":";".join(checkpoint("spark-bronze",f"/opt/project-data/checkpoints/{x}") for x in ["air_quality","openaq_measurements","weather"]),
        "silver_offsets":";".join(checkpoint("spark-silver",f"/opt/project-data/checkpoints/silver/{x}") for x in ["air_quality","openaq_measurements","weather"]),
        "gold_offsets":checkpoint("spark-gold","/opt/project-data/checkpoints/gold_notifications_v5"),
        "silver_nonempty_batch_logs":" || ".join(x.strip() for x in logs["spark-silver"] if "Processed Silver batch" in x),
        "silver_notifications":" || ".join(x.strip() for x in logs["spark-silver"] if "Published Silver batch notification" in x),
        "gold_batch_write_logs":" || ".join(x.strip() for x in logs["spark-gold"] if "Wrote notification-driven Gold batch" in x),
        "postgres_load_logs":" || ".join(x.strip() for x in logs["postgres-loader"] if "Loaded Gold batch into PostgreSQL" in x),
        "gold_ledger":sql("SELECT count(*)||'|'||coalesce(max(batch_id)::text,'')||'|'||coalesce(max(processed_at)::text,'') FROM processed_gold_batches"),
        "fact_rows_latest":sql("SELECT count(*)||'|'||coalesce(max(updated_at)::text,'') FROM fact_hourly_measurements"),
        "container_states":inspect(),
        "bronze_trigger_overrun":any("falling behind" in x.lower() or "trigger interval" in x.lower() and "spent" in x.lower() for x in logs["spark-bronze"]),
        "silver_errors":any("exception" in x.lower() or "error" in x.lower() for x in logs["spark-silver"]),
        "gold_errors":any("exception" in x.lower() or "error" in x.lower() for x in logs["spark-gold"]),
        "bridge_blocking":any("event loop blocking" in x.lower() for x in logs["mqtt-kafka-bridge"]),
        "bridge_delivery_fail":any("delivery failed" in x.lower() or "queue full" in x.lower() for x in logs["mqtt-kafka-bridge"])}
        wr.writerow(rec); f.flush()
        print(json.dumps(rec,ensure_ascii=False),flush=True)
        if i<20: time.sleep(60)
print("SOAK_PART2_COMPLETE",str(OUT),flush=True)
