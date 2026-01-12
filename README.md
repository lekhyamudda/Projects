# Projects

## Population API Fetcher

This repo includes a script that fetches population data from a public API and uploads the
JSON payload to S3.

### Requirements

* Python 3.9+
* Dependencies: `boto3`, `requests`

Install dependencies:

```bash
pip install -r requirements.txt
```

### Environment variables

* `POPULATION_API_URL` (optional): Override the API URL. Defaults to Data USA.
* `POPULATION_S3_BUCKET` (required): Target S3 bucket.
* `POPULATION_S3_KEY` (optional): Object key to write. Default: `api/population.json`.
* `POPULATION_API_TIMEOUT` (optional): Request timeout in seconds. Default: `10`.

Standard AWS variables are also required for authentication (e.g., `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` or an equivalent credential provider).

### Run

```bash
python scripts/fetch_population.py
```

To append a timestamp to the S3 object key:

```bash
python scripts/fetch_population.py --timestamp-key
```
