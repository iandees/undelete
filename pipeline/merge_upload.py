"""Upload PMTiles to Cloudflare R2."""

import logging
from pathlib import Path

import boto3

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
