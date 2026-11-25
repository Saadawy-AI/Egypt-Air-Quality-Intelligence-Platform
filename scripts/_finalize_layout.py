from pathlib import Path
import os, re, subprocess
root=Path(__file__).resolve().parents[1]
os.chdir(root)

# Flatten the Wokwi project one level: ingestion/iot/wokwi/esp32-air-quality-node.
outer=Path("ingestion/iot/wokwi/esp32-air-quality-node")
inner=outer/"esp32-air-quality-node"
if inner.exists():
    for item in list(inner.iterdir()):
        target=outer/item.name
        if target.exists(): raise RuntimeError(f"Wokwi target exists: {target}")
        subprocess.run(["git","mv",str(item),str(target)],check=True)
    inner.rmdir()

# Repair Azure guide links after documentation classification.
p=Path("infra/azure/README.md")
s=p.read_text(encoding="utf-8")
s=s.replace("../../docs/AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md","../../docs/architecture/AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md")
s=s.replace("../../docs/AZURE_READINESS_REVIEW_2026-11-25.md","../../docs/reports/AZURE_READINESS_REVIEW_2026-11-25.md")
p.write_text(s,encoding="utf-8",newline="\n")

# Remove only temporary migration/verification helpers created during this operation.
for name in ["_restructure_layout.py","_fix_layout.py","_fix_layout2.py","_verify_layout.py"]:
    q=Path("scripts")/name
    if q.exists(): q.unlink()

# Verify all local Markdown links.
bad=[]
pat=re.compile(r'!?\[[^\]]*\]\(([^)]+)\)')
for f in root.rglob("*.md"):
    if any(x in f.parts for x in (".git",".venv","backups")): continue
    body=f.read_text(encoding="utf-8",errors="ignore")
    for m in pat.finditer(body):
        target=m.group(1).strip()
        if target.startswith(("http://","https://","mailto:","#","data:")): continue
        raw=target.split("#",1)[0]
        if raw and not (f.parent/raw.replace("\\","/")).resolve().exists():
            bad.append((f.as_posix(),target))
if bad: raise RuntimeError("Broken Markdown links: "+repr(bad[:20]))

required=[
"ingestion/openaq","ingestion/open_meteo_air_quality","ingestion/open_meteo_weather","ingestion/iot/wokwi/esp32-air-quality-node","ingestion/iot/simulator",
"kafka/config","kafka/schemas","kafka/topics","kafka/bridge","spark/bronze","spark/silver","spark/gold","spark/jobs",
"data/raw","data/bronze","data/silver","data/gold","data/reference","data/quarantine",
"airflow/dags","airflow/config","airflow/plugins","postgres/migrations","postgres/schemas","postgres/loaders","postgres/tests",
"streamlit/pages","streamlit/components","streamlit/assets","ml/training","ml/inference","ml/evaluation","ml/models",
"monitoring/dashboards","monitoring/alerts","monitoring/exporters","testing/unit","testing/integration","testing/end_to_end","testing/performance","testing/acceptance",
"infra/docker","infra/azure","docs/architecture","docs/guides","docs/operations","docs/reports"]
missing=[x for x in required if not Path(x).is_dir()]
if missing: raise RuntimeError("Missing required directories: "+repr(missing))
if not Path("ml/models/pm25").exists(): raise RuntimeError("Model artifacts missing")
if not Path("data/checkpoints").exists(): raise RuntimeError("Streaming checkpoints missing")

subprocess.run(["docker","compose","config","-q"],check=True)
subprocess.run(["git","add","-A"],check=True)
subprocess.run(["git","diff","--cached","--check"],check=True)
staged=subprocess.check_output(["git","diff","--cached","--name-only"],text=True).splitlines()
if any(x in {".env",".env.airflow-jwt"} for x in staged):
    raise RuntimeError("Refusing to stage local credentials")
subprocess.run(["git","commit","-m","Reorganize repository by platform components"],check=True)
subprocess.run(["git","push","origin","main"],check=True)
print("Commit and push completed.")
print("Markdown links: PASS; required directories: PASS; Compose config: PASS.")
print("Local model artifacts and data/checkpoints preserved.")
print(subprocess.check_output(["git","status","--short","--branch"],text=True))
