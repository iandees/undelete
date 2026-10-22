"""Upload PMTiles to Cloudflare R2."""

import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class R2Uploader:
    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, local_path: Path, remote_key: str):
        """Upload a file to R2."""
        logger.info("Uploading %s -> s3://%s/%s", local_path, self.bucket_name, remote_key)
        self.client.upload_file(str(local_path), self.bucket_name, remote_key)

    def list_dates(self, prefix: str) -> list[str]:
        """Dates that have a published `<prefix>/date=<date>/data.parquet`, sorted."""
        dates = set()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=f"{prefix}/date="):
            for obj in page.get("Contents", []):
                parts = obj["Key"].split("/")
                if len(parts) == 3 and parts[2] == "data.parquet":
                    dates.add(parts[1].split("=", 1)[1])
        return sorted(dates)

    def download_if_exists(self, remote_key: str, local_path: Path) -> bool:
        """Download an object to `local_path`. Returns False if it does not exist.

        Any other failure (network, credentials) raises: treating "could not reach R2" as "no
        such file" would let a caller build a fresh file over a published one.
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=remote_key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        logger.info("Downloading s3://%s/%s -> %s", self.bucket_name, remote_key, local_path)
        self.client.download_file(self.bucket_name, remote_key, str(local_path))
        return True
