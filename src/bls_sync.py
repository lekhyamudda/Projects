from __future__ import annotations

import argparse
import dataclasses
import logging
import time
from html.parser import HTMLParser
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import boto3
import botocore
import requests

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
DEFAULT_RATE_LIMIT_SECONDS = 0.5
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; bls-pr-sync/1.0; +https://download.bls.gov/)"
)


@dataclasses.dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)


class BLSClient:
    def __init__(self, base_url: str, user_agent: str, rate_limit: float) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self.rate_limit = rate_limit

    def _sleep(self) -> None:
        if self.rate_limit > 0:
            time.sleep(self.rate_limit)

    def list_remote_files(self) -> List[RemoteFile]:
        response = self.session.get(self.base_url, timeout=30)
        response.raise_for_status()
        parser = _LinkParser()
        parser.feed(response.text)
        files: List[RemoteFile] = []
        for link in parser.links:
            if link.endswith("/"):
                continue
            parsed = urlparse(link)
            if parsed.scheme or parsed.netloc:
                url = link
            else:
                url = urljoin(self.base_url, link)
            name = link.split("/")[-1]
            if not name:
                continue
            files.append(RemoteFile(name=name, url=url))
        self._sleep()
        return sorted(files, key=lambda item: item.name)

    def head(self, remote_file: RemoteFile) -> requests.Response:
        response = self.session.head(remote_file.url, timeout=30, allow_redirects=True)
        response.raise_for_status()
        self._sleep()
        return response

    def download(self, remote_file: RemoteFile) -> requests.Response:
        response = self.session.get(remote_file.url, timeout=60, stream=True)
        response.raise_for_status()
        return response


@dataclasses.dataclass
class RemoteMetadata:
    size: int
    last_modified: Optional[str]
    etag: Optional[str]


class S3Syncer:
    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3")

    def _key_for(self, filename: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{filename}"
        return filename

    def list_objects(self) -> dict[str, dict]:
        paginator = self.s3.get_paginator("list_objects_v2")
        objects: dict[str, dict] = {}
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for item in page.get("Contents", []):
                objects[item["Key"]] = item
        return objects

    def head_object(self, key: str) -> Optional[dict]:
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "404":
                return None
            raise

    def upload(
        self,
        remote_file: RemoteFile,
        metadata: RemoteMetadata,
        body: Iterable[bytes],
    ) -> None:
        key = self._key_for(remote_file.name)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            Metadata={
                "bls-last-modified": metadata.last_modified or "",
                "bls-etag": metadata.etag or "",
            },
        )

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)


def _metadata_from_head(head: requests.Response) -> RemoteMetadata:
    size = int(head.headers.get("Content-Length", "0"))
    last_modified = head.headers.get("Last-Modified")
    etag = head.headers.get("ETag")
    return RemoteMetadata(size=size, last_modified=last_modified, etag=etag)


def _s3_metadata_matches(head: RemoteMetadata, obj_head: dict) -> bool:
    if obj_head.get("ContentLength") != head.size:
        return False
    metadata = obj_head.get("Metadata", {})
    if head.last_modified and metadata.get("bls-last-modified") != head.last_modified:
        return False
    if head.etag and metadata.get("bls-etag") != head.etag:
        return False
    return True


def sync(
    *,
    bucket: str,
    prefix: str,
    base_url: str,
    user_agent: str,
    rate_limit: float,
    delete_missing: bool,
    logger: logging.Logger,
) -> None:
    client = BLSClient(base_url=base_url, user_agent=user_agent, rate_limit=rate_limit)
    s3_syncer = S3Syncer(bucket=bucket, prefix=prefix)

    remote_files = client.list_remote_files()
    logger.info("Remote file count: %s", len(remote_files))
    s3_objects = s3_syncer.list_objects()
    logger.info("S3 object count: %s", len(s3_objects))

    remote_keys = set()
    for remote_file in remote_files:
        key = s3_syncer._key_for(remote_file.name)
        remote_keys.add(key)
        logger.info("Checking %s", remote_file.name)
        head = client.head(remote_file)
        remote_meta = _metadata_from_head(head)

        obj_head = s3_syncer.head_object(key)
        if obj_head and _s3_metadata_matches(remote_meta, obj_head):
            logger.info("No change for %s", remote_file.name)
            continue

        logger.info("Downloading %s", remote_file.name)
        response = client.download(remote_file)
        logger.info("Uploading %s to s3://%s/%s", remote_file.name, bucket, key)
        s3_syncer.upload(remote_file, remote_meta, response.iter_content(chunk_size=1024 * 1024))

    if delete_missing:
        for key in s3_objects:
            if key not in remote_keys:
                logger.info("Deleting stale object %s", key)
                s3_syncer.delete(key)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync BLS PR dataset files to S3.")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name.")
    parser.add_argument(
        "--prefix",
        default="bls/pr",
        help="S3 prefix to store files (default: bls/pr).",
    )
    parser.add_argument(
        "--base-url",
        default=BLS_BASE_URL,
        help="BLS base URL to crawl for files.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to satisfy BLS policy.",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT_SECONDS,
        help="Seconds to sleep between requests.",
    )
    parser.add_argument(
        "--delete-missing",
        action="store_true",
        help="Delete S3 objects that no longer exist upstream.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("bls-sync")

    sync(
        bucket=args.bucket,
        prefix=args.prefix,
        base_url=args.base_url,
        user_agent=args.user_agent,
        rate_limit=args.rate_limit,
        delete_missing=args.delete_missing,
        logger=logger,
    )


if __name__ == "__main__":
    main()
