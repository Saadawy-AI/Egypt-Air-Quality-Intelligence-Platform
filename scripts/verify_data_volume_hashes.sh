#!/bin/sh
set -eu

for directory in bronze silver gold checkpoints; do
  source_hash=$(cd "/src/$directory" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
  volume_hash=$(cd "/dst/$directory" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
  echo "$directory source=$source_hash"
  echo "$directory volume=$volume_hash"
  [ "$source_hash" = "$volume_hash" ] || {
    echo "HASH_MISMATCH: $directory"
    exit 2
  }
done

echo "PASS: Bronze, Silver, Gold, and Spark checkpoint SHA-256 manifests match."
