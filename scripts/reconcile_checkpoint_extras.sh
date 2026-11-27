#!/bin/sh
set -eu

ARCHIVE="/dst/_migration_extras_20261127T0715Z"
find /src/checkpoints -type f | sed 's#^/src/##' | sort > /tmp/src-checkpoints.txt
find /dst/checkpoints -type f | sed 's#^/dst/##' | sort > /tmp/dst-checkpoints.txt

comm -23 /tmp/src-checkpoints.txt /tmp/dst-checkpoints.txt > /tmp/missing-checkpoints.txt
comm -13 /tmp/src-checkpoints.txt /tmp/dst-checkpoints.txt > /tmp/extra-checkpoints.txt

SRC_COUNT=$(wc -l < /tmp/src-checkpoints.txt)
DST_COUNT=$(wc -l < /tmp/dst-checkpoints.txt)
MISSING_COUNT=$(wc -l < /tmp/missing-checkpoints.txt)
EXTRA_COUNT=$(wc -l < /tmp/extra-checkpoints.txt)

echo "source_checkpoints=$SRC_COUNT destination_checkpoints=$DST_COUNT missing=$MISSING_COUNT extra=$EXTRA_COUNT"
if [ "$MISSING_COUNT" -ne 0 ]; then
  echo "Missing checkpoint files; refusing to archive or switch."
  head -50 /tmp/missing-checkpoints.txt
  exit 2
fi

while IFS= read -r relative; do
  [ -n "$relative" ] || continue
  mkdir -p "$ARCHIVE/$(dirname "$relative")"
  mv "/dst/$relative" "$ARCHIVE/$relative"
done < /tmp/extra-checkpoints.txt

FINAL_COUNT=$(find /dst/checkpoints -type f | wc -l)
echo "active_checkpoints_after_archive=$FINAL_COUNT"
echo "archived_checkpoint_extras=$EXTRA_COUNT at $ARCHIVE"
[ "$FINAL_COUNT" -eq "$SRC_COUNT" ]
