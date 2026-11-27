#!/usr/bin/env python3
"""Reconcile a copied project data tree against its stopped Windows source.

Missing or metadata-mismatched files are copied from source. Extra destination
files are preserved under _migration_extras_... rather than deleted. All files
under bronze/, silver/, gold/, and checkpoints/ are SHA-256 verified.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import stat

SOURCE = "/src"
DESTINATION = "/dst"
ARCHIVE = os.path.join(DESTINATION, "_migration_extras_20261127T0715Z")


def collect(root: str, exclude_archive: bool = False) -> dict[str, tuple]:
    entries: dict[str, tuple] = {}
    for base, dirs, names in os.walk(root, followlinks=False):
        if exclude_archive:
            dirs[:] = [name for name in dirs if not name.startswith("_migration_extras_")]
        for name in names:
            path = os.path.join(base, name)
            relative = os.path.relpath(path, root)
            if os.path.islink(path):
                entries[relative] = ("link", os.readlink(path))
            elif os.path.isfile(path):
                info = os.stat(path, follow_symlinks=False)
                entries[relative] = (
                    "file", info.st_size, info.st_mtime_ns,
                    info.st_mode, info.st_uid, info.st_gid,
                )
    return entries


def copy_preserving(source: str, destination: str) -> None:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    shutil.copy2(source, destination)
    info = os.stat(source, follow_symlinks=False)
    os.chown(destination, info.st_uid, info.st_gid)
    os.chmod(destination, stat.S_IMODE(info.st_mode))


def sha256(path: str) -> bytes:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


print("Collecting source and destination manifests...", flush=True)
source = collect(SOURCE)
destination = collect(DESTINATION, exclude_archive=True)
source_paths = set(source)
destination_paths = set(destination)
missing = sorted(source_paths - destination_paths)
extras = sorted(destination_paths - source_paths)
print(
    f"source={len(source_paths)} destination={len(destination_paths)} "
    f"missing={len(missing)} extras={len(extras)}",
    flush=True,
)
print("extra_sample:", extras[:25], flush=True)

fixed_missing = 0
fixed_metadata = 0

for relative in missing:
    src = os.path.join(SOURCE, relative)
    dst = os.path.join(DESTINATION, relative)
    if os.path.islink(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.lexists(dst):
            os.unlink(dst)
        os.symlink(os.readlink(src), dst)
        source_info = os.lstat(src)
        if hasattr(os, "lchown"):
            os.lchown(dst, source_info.st_uid, source_info.st_gid)
    else:
        copy_preserving(src, dst)
    fixed_missing += 1

for relative in sorted(source_paths & destination_paths):
    src = os.path.join(SOURCE, relative)
    dst = os.path.join(DESTINATION, relative)
    if os.path.islink(src):
        if not os.path.islink(dst) or os.readlink(src) != os.readlink(dst):
            if os.path.lexists(dst):
                os.unlink(dst)
            os.symlink(os.readlink(src), dst)
            source_info = os.lstat(src)
            if hasattr(os, "lchown"):
                os.lchown(dst, source_info.st_uid, source_info.st_gid)
            fixed_metadata += 1
        continue

    source_info = os.stat(src, follow_symlinks=False)
    destination_info = os.stat(dst, follow_symlinks=False)
    source_metadata = (
        source_info.st_size, source_info.st_mtime_ns,
        source_info.st_mode, source_info.st_uid, source_info.st_gid,
    )
    destination_metadata = (
        destination_info.st_size, destination_info.st_mtime_ns,
        destination_info.st_mode, destination_info.st_uid, destination_info.st_gid,
    )
    if source_metadata != destination_metadata:
        copy_preserving(src, dst)
        fixed_metadata += 1

critical = sorted(
    relative for relative in source_paths
    if relative.startswith(("bronze/", "silver/", "gold/", "checkpoints/"))
    and source[relative][0] == "file"
)
mismatches: list[str] = []
for index, relative in enumerate(critical, 1):
    src = os.path.join(SOURCE, relative)
    dst = os.path.join(DESTINATION, relative)
    if sha256(src) != sha256(dst):
        copy_preserving(src, dst)
        if sha256(src) != sha256(dst):
            mismatches.append(relative)
    if index % 10000 == 0:
        print(f"SHA-256 verified {index}/{len(critical)} critical files", flush=True)

if mismatches:
    raise SystemExit(f"Critical SHA-256 mismatches remain: {mismatches[:20]}")

for relative in extras:
    src = os.path.join(DESTINATION, relative)
    dst = os.path.join(ARCHIVE, relative)
    if os.path.lexists(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        os.replace(src, dst)

final_entries = collect(DESTINATION, exclude_archive=True)
final_paths = set(final_entries)
missing_final = sorted(source_paths - final_paths)
extra_final = sorted(final_paths - source_paths)
if missing_final or extra_final:
    raise SystemExit(
        f"Final manifest mismatch: missing={missing_final[:10]}, "
        f"extras={extra_final[:10]}"
    )

print(
    f"PASS: {len(source_paths)} active entries match; "
    f"{len(critical)} critical files SHA-256 verified.",
    flush=True,
)
print(
    f"Repaired missing={fixed_missing}, metadata={fixed_metadata}; "
    f"archived extras={len(extras)}; hash mismatches remaining=0.",
    flush=True,
)
print(f"Archived extras: {ARCHIVE}", flush=True)
