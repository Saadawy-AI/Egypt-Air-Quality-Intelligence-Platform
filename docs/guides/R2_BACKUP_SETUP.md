# Cloudflare R2 backup integration

## Credentials required (S3 API credentials, not the general Cloudflare API token)

1. Open Cloudflare Dashboard -> R2 Object Storage -> Manage R2 API Tokens.
2. Create an R2 token with **Object Read & Write** permissions.
3. Scope it to the existing bucket **egypt-air-quality** only.
4. Copy the generated **Access Key ID** and **Secret Access Key** immediately; the secret is shown only once.
5. Get the Cloudflare **Account ID** from the dashboard account details. Do not use the bucket name or API token value as the account ID.

Do not paste credentials into chat, Git, documentation, or command arguments.

## Configure local environment

Edit the ignored project `.env` file locally and add:

```dotenv
R2_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_r2_access_key_id
R2_SECRET_ACCESS_KEY=your_r2_secret_access_key
R2_BUCKET=egypt-air-quality
```

The same variable names are documented in `.env.example`. The real `.env` is ignored by Git and Docker build context.

## Install the backup client

From the project root in WSL:

```bash
.venv/bin/pip install -r scripts/requirements-r2.txt
```

## Upload and verify

Run from the project root:

```bash
set -a
source ./.env
set +a
.venv/bin/python scripts/r2_backup.py upload
```

The uploader selects only recovery-oriented `.dump`, `.tar`, `.md`, and `.txt` files under `backups/`; it does not upload `.env`, credentials, project source, or arbitrary data directories. Each object is tagged with its SHA-256 and source size, then checked using R2 HEAD metadata and Content-Length.

List uploaded objects:

```bash
.venv/bin/python scripts/r2_backup.py list
```

## Restore verification (non-destructive)

Choose an exact object key from the list, then:

```bash
.venv/bin/python scripts/r2_backup.py verify-download egypt-air-quality/backups/2026-11-24/airquality.dump
```

This downloads into a temporary directory, compares SHA-256 against the metadata written at upload, and deletes only that temporary directory on exit. It does not overwrite local backups or live databases. For a PostgreSQL dump, additionally run `pg_restore --list` on the downloaded file in a controlled restore drill. For a tar archive, the script checks that the archive is readable and non-empty.

## Operational policy

- Keep local backups; R2 is the off-host copy, not a replacement.
- Do not upload secrets, `.env`, JWT keys, or raw credentials.
- Keep the R2 token restricted to this bucket and only the required object operations.
- Do not add an automatic deletion lifecycle until retention and restore tests are approved.
- A successful upload/HEAD check is not a restore test. Record a successful download+SHA-256 verification and PostgreSQL/archive validation separately.
