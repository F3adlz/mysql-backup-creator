import dataclasses
import logging
import threading
from dataclasses import field
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

MB = 1024 * 1024


@dataclasses.dataclass
class S3Config:
    endpoint: str
    region: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    bucket: str


class Uploader:

    def __init__(self, config: S3Config):
        self.client = threading.local()
        self.client = boto3.session.Session().resource(
            's3', endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region)
        self.bucket = config.bucket
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = TransferConfig(multipart_threshold=MB * 15,
                                     max_concurrency=2,
                                     multipart_chunksize=MB * 15)

    def upload(self, path: Path, key: str):
        try:
            self.client.Bucket(self.bucket).upload_file(
                path, key, Config=self.config)
            self.logger.debug(f"File {path} uploaded to {self.bucket}")
        except ClientError as e:
            message = f"Unable to upload backup {path} to {self.bucket}"
            raise UploaderError(message) from e


class UploaderError(Exception):
    pass
