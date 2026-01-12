#!/usr/bin/env python3
"""Fetch population data from an API and upload to S3."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

DEFAULT_API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
DEFAULT_S3_KEY = "api/population.json"
DEFAULT_TIMEOUT = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch population data and upload it to S3 as JSON.",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("POPULATION_API_URL", DEFAULT_API_URL),
        help="Population API URL (env: POPULATION_API_URL).",
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.getenv("POPULATION_S3_BUCKET"),
        help="S3 bucket name (env: POPULATION_S3_BUCKET).",
    )
    parser.add_argument(
        "--s3-key",
        default=os.getenv("POPULATION_S3_KEY", DEFAULT_S3_KEY),
        help=f"S3 object key (env: POPULATION_S3_KEY, default: {DEFAULT_S3_KEY}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("POPULATION_API_TIMEOUT", DEFAULT_TIMEOUT)),
        help=f"Request timeout in seconds (env: POPULATION_API_TIMEOUT, default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--timestamp-key",
        action="store_true",
        help="Append a timestamp to the S3 object key.",
    )
    return parser.parse_args()


def build_s3_key(base_key: str, timestamped: bool) -> str:
    if not timestamped:
        return base_key

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if "." in base_key:
        stem, ext = base_key.rsplit(".", 1)
        return f"{stem}_{timestamp}.{ext}"
    return f"{base_key}_{timestamp}"


def fetch_population(api_url: str, timeout: int) -> dict[str, Any]:
    response = requests.get(api_url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Population API request failed with status {response.status_code}: {response.text}"
        )
    return response.json()


def serialize_payload(api_url: str, data: dict[str, Any]) -> bytes:
    payload = {
        "source_url": api_url,
        "data": data,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def upload_to_s3(bucket: str, key: str, body: bytes) -> None:
    client = boto3.client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def main() -> int:
    args = parse_args()

    if not args.s3_bucket:
        print("POPULATION_S3_BUCKET (or --s3-bucket) is required.", file=sys.stderr)
        return 2

    try:
        data = fetch_population(args.api_url, args.timeout)
        payload_bytes = serialize_payload(args.api_url, data)
        s3_key = build_s3_key(args.s3_key, args.timestamp_key)
        upload_to_s3(args.s3_bucket, s3_key, payload_bytes)
    except requests.Timeout as exc:
        print(f"Population API request timed out: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Population API request error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, json.JSONDecodeError, BotoCoreError, ClientError) as exc:
        print(f"Failed to fetch or upload population data: {exc}", file=sys.stderr)
        return 1

    print(f"Uploaded population data to s3://{args.s3_bucket}/{s3_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
