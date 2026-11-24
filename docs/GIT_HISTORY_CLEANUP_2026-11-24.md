# Git History and Workspace Cleanup — 2026-11-24

## GitHub history remediation

The pre-cleanup Gitleaks report found 9 generic API-key findings in historical `.env` and `.env.example` snapshots. The real local credentials were rotated before this history rewrite.

Completed:
- Rewrote all GitHub branches: `main`, `fix/kafka-persistence`, and `master`.
- Removed `.env` and `.env.airflow-jwt` from all reachable commits.
- Scrubbed non-empty credential-bearing assignments (passwords, API keys, tokens, secrets) from every reachable historical `.env.example` blob while retaining the template file.
- Removed local agent checkpoint refs from the active repository.
- Expired reflogs and garbage-collected unreachable pre-rewrite objects from the active repository.
- Force-updated all three GitHub branches and verified the remote branch hashes:
  - `main`: `159c8e16183277f541b1857b5f22a80d2cbaae6c`
  - `fix/kafka-persistence`: `60db0444ac25850d04d87d476284cf108f1609f2`
  - `master`: `dda8754fb86ec4899bb9e02644c91039f51425aa`

Post-rewrite verification against all reachable history:
- Reachable commits scanned: 9.
- Non-empty secret-like assignments in historical `.env.example`: 0.
- Reachable `.env` / `.env.airflow-jwt` paths: 0.
- A sanitized Git bundle was created and verified at `backups/git-cleanup-preflight-20261124/all-refs-after-full-secret-scrub.bundle`.
- The pre-rewrite Git bundle and superseded local Git metadata were deleted after the remote rewrite was verified; no plaintext pre-rewrite Git backup remains in the project.

The old Gitleaks JSON report remains as redacted pre-cleanup evidence only. It is not the current scan result. A standalone Gitleaks binary was not available for a post-rewrite rerun; the verification above directly inspected reachable Git objects and all historical environment-template assignments.

## Duplicate workspace removal

Removed `D:\Projects\egypt_air_quality\Egypt-Air-Quality-Intelligence-Platform-GitHub`.
- Its clean branch HEAD was identical to the primary repository's `origin/fix/kafka-persistence` commit.
- Its only useful local-only files were three pre-current-Gold snapshots; these were preserved under `backups/legacy-pre-current-gold-20261121/`.
- Its old `.env` contained a different (stale) OpenAQ key from the active project; the duplicate folder and stale local credentials were deleted.
- Its old `data/ivy2` was only a Spark dependency cache and was not needed as a second project copy.

## Workspace hygiene

- Removed the stale root-level `PROJECT_AUDIT_REPORT.txt` and one-off `test_openaq_connection.py`; the maintained pytest suites and Airflow ingestion smoke test cover the connection path.
- Removed 26 Python/test cache directories (`__pycache__`, `.pytest_cache`, etc.).
- Removed tracked `data/ivy2` cache files from the Git index while retaining the local cache directory for running Spark. The path is ignored by `.gitignore`.
- Existing backups, production Parquet data, checkpoints, Docker volumes, and model artifacts were not deleted.
- The original working tree changes were preserved in a patch and an archive of 55 untracked files under `backups/git-cleanup-preflight-20261124/`; the patch excludes ignored secret files.

## OpenAQ credential verification

- Host `.env`, Airflow scheduler, and Airflow API server have matching OpenAQ key fingerprints (fingerprint only; no key value recorded).
- Direct OpenAQ v3 request from the scheduler returned HTTP 200.
- All 16 project Compose services were Up at the time of verification.

## Remaining caveats

- A post-rewrite Gitleaks binary scan should be run once the tool is available; current object-level checks show no reachable private environment files or non-empty secret-bearing template assignments.
- Git history was rewritten, so any other clones must be freshly cloned or hard-reset to the new branch history; do not merge old branches containing the exposed commits back into GitHub.
- GitHub force-push changed commit IDs. Existing pull requests, branch protections, or collaborators may need synchronization.
- This cleanup does not by itself approve Azure readiness. Host/WSL memory pressure, sustained soak/fault-injection testing, real MQTT sensor payload validation, and production Kafka TLS/SASL/HA remain separate gates.
