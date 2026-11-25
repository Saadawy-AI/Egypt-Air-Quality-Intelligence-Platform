#!/usr/bin/env python3
"""Upload and verify project recovery artifacts in Cloudflare R2 (S3 API)."""
from __future__ import annotations
import argparse, hashlib, os, tempfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
BACKUP_ROOT = ROOT / "backups"

def client_and_bucket():
    required = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit("Missing environment variables: " + ", ".join(missing))
    endpoint = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    client = boto3.client("s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    return client, os.environ["R2_BUCKET"]

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def candidates():
    allowed = {".dump", ".tar", ".md", ".txt"}
    for path in sorted(BACKUP_ROOT.rglob("*")):
        if path.is_file() and path.suffix.lower() in allowed and not path.name.endswith("redacted-report.json"):
            yield path

def upload(args):
    s3, bucket = client_and_bucket()
    files = list(candidates())
    if args.since_hours is not None:
        cutoff = __import__("time").time() - args.since_hours * 3600
        files = [path for path in files if path.stat().st_mtime >= cutoff]
    if not files:
        raise SystemExit(f"No eligible backup artifacts found under {BACKUP_ROOT}")
    for path in files:
        digest = sha256_file(path)
        key = f"{args.prefix.strip('/')}/{path.relative_to(BACKUP_ROOT).as_posix()}"
        s3.upload_file(str(path), bucket, key,
            ExtraArgs={"Metadata": {"sha256": digest, "source_size": str(path.stat().st_size)}})
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("Metadata", {}).get("sha256") != digest or head.get("ContentLength") != path.stat().st_size:
            raise RuntimeError(f"Remote HEAD verification failed for {key}")
        # Verify the stored bytes, not only the metadata supplied at upload.
        with tempfile.TemporaryDirectory(prefix="r2-upload-verify-") as temp:
            downloaded = Path(temp) / path.name
            s3.download_file(bucket, key, str(downloaded))
            downloaded_digest = sha256_file(downloaded)
            if downloaded_digest != digest or downloaded.stat().st_size != path.stat().st_size:
                raise RuntimeError(f"Remote download/SHA-256 verification failed for {key}")
        print(f"VERIFIED {key} size={head['ContentLength']} sha256={digest} download_sha256=match")
    print(f"SUCCESS: {len(files)} objects uploaded, downloaded and SHA-256 verified in bucket {bucket}.")

def verify_download(args):
    s3, bucket = client_and_bucket()
    with tempfile.TemporaryDirectory(prefix="r2-restore-check-") as temp:
        target = Path(temp) / Path(args.key).name
        s3.download_file(bucket, args.key, str(target))
        digest = sha256_file(target)
        head = s3.head_object(Bucket=bucket, Key=args.key)
        expected = head.get("Metadata", {}).get("sha256")
        if not expected or digest != expected:
            raise RuntimeError(f"SHA-256 mismatch for {args.key}")
        print(f"DOWNLOAD+SHA256 VERIFIED: {args.key} ({target.stat().st_size} bytes, {digest})")
        if target.suffix.lower() == ".tar":
            import tarfile
            with tarfile.open(target, "r:*") as archive:
                members = archive.getmembers()
                if not members:
                    raise RuntimeError("Downloaded tar archive is empty")
                print(f"ARCHIVE VERIFIED: {len(members)} entries readable")
        elif target.suffix.lower() == ".dump":
            print("Checksum verified; run pg_restore --list for PostgreSQL format validation.")
    print("SUCCESS: temporary download verified; no live data was modified.")

def list_objects(args):
    s3, bucket = client_and_bucket()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=args.prefix.strip("/") + "/"):
        for obj in page.get("Contents", []):
            print(f"{obj['Key']} {obj['Size']} bytes")
            count += 1
    print(f"TOTAL: {count} objects")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("upload"); p.add_argument("--prefix", default="egypt-air-quality/backups"); p.add_argument("--since-hours", type=float, default=None); p.set_defaults(func=upload)
    p = sub.add_parser("verify-download"); p.add_argument("key"); p.set_defaults(func=verify_download)
    p = sub.add_parser("list"); p.add_argument("--prefix", default="egypt-air-quality/backups"); p.set_defaults(func=list_objects)
    args = parser.parse_args(); args.func(args)

if __name__ == "__main__":
    main()
