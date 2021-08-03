from typing import Optional

import boto3
from os import path, makedirs
from uuid import uuid4

from app.cli import log
from app.config import S3_EVE_BUCKET


class BaseMigrationService:
    """Base migration service."""

    def __init__(self):
        self.job_id: Optional[str] = None
        self.tag = ''
        self.temp_inp_dir: Optional[str] = None
        self.temp_out_dir: Optional[str] = None

        # Initialize S3 bucket
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(S3_EVE_BUCKET)

    def start_job(self):
        """Start a new migration job."""

        self.job_id = str(uuid4())
        self.tag = f'[{self.job_id}] '

        # Create temp directories for downloaded and migrated files
        self.temp_inp_dir = path.join('temp', 'inputs', self.job_id)
        makedirs(self.temp_inp_dir, exist_ok=True)

        self.temp_out_dir = path.join('temp', 'outputs', self.job_id)
        makedirs(self.temp_out_dir, exist_ok=True)

        log(f'{self.tag}🚀 Migration job started.')

    def succeed(self):
        log(f'{self.tag}🎉 Migration job completed successfully.')
