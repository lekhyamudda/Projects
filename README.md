# BLS PR Dataset Sync

This project syncs the Bureau of Labor Statistics (BLS) PR time series files to S3. It discovers the files by parsing the directory listing at `https://download.bls.gov/pub/time.series/pr/` and keeps your S3 prefix aligned with the upstream listing.

## Project layout

```
.
├── requirements.txt
├── scripts/
│   └── sync_bls_pr.py
└── src/
    └── bls_sync.py
```

## Requirements

- Python 3.10+
- AWS credentials with permission to list, read, write, and delete objects in the target bucket

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## AWS credentials

The script uses the default AWS credential chain. You can provide credentials with any of the following methods:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
- `~/.aws/credentials` and `~/.aws/config`
- An EC2/ECS role with the right S3 permissions

## Usage

Run the sync with your bucket and prefix:

```bash
python scripts/sync_bls_pr.py --bucket your-bucket --prefix bls/pr
```

Optional flags:

- `--delete-missing` to remove S3 objects that no longer exist upstream.
- `--rate-limit` to slow requests to the BLS host (default 0.5 seconds).
- `--user-agent` to provide a custom User-Agent string (required by BLS; avoid 403s).

Example with deletion enabled:

```bash
python scripts/sync_bls_pr.py --bucket your-bucket --prefix bls/pr --delete-missing
```

## Notes on BLS access policy

BLS may return 403 responses if requests lack a reasonable `User-Agent`. The sync script sends a descriptive `User-Agent` along with common headers and throttles requests to respect the directory host. Adjust `--rate-limit` if you see throttling or want to be more conservative.
