from pathlib import Path
from r2_backup import client_and_bucket, sha256_file

KEY = "egypt-air-quality/backups/2026-11-24/airquality.dump"
TARGET = Path(__file__).resolve().parents[1] / "backups" / "r2-restore-drill" / "airquality.dump"
TARGET.parent.mkdir(parents=True, exist_ok=True)
s3, bucket = client_and_bucket()
s3.download_file(bucket, KEY, str(TARGET))
head = s3.head_object(Bucket=bucket, Key=KEY)
actual = sha256_file(TARGET)
expected = head.get("Metadata", {}).get("sha256")
if not expected or actual != expected or TARGET.stat().st_size != head["ContentLength"]:
    raise SystemExit("FAIL: downloaded backup size/SHA-256 mismatch")
print(f"DOWNLOAD_SHA256_OK size={TARGET.stat().st_size} sha256={actual} path={TARGET}")
