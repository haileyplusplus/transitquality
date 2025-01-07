import threading

from s3path import S3Path
from pathlib import Path
import boto3
import botocore.exceptions
import tempfile
import json
from typing import Iterable


class RouteInterpolate:
    BUCKET = S3Path('/transitquality2024/bustracker/raw')

    def __init__(self):
        #self.workdir = tempfile.TemporaryDirectory()
        self.workpath = Path('/transitworking')
        try:
            boto3.setup_default_session(profile_name='transitquality_boto')
        except botocore.exceptions.ProfileNotFound:
            print(f'Not using boto profile')
        #with pattern_file.open() as jfh:
        #    self.patterns = json.load(jfh)
        self.load_working()

    def load_working(self):
        # TODO: finer grained date parsing
        bundles: Iterable[S3Path] = self.BUCKET.glob('bundle-2025????.tar.lz')
        pattern_file = self.BUCKET / 'patterns2025.json'
        items = list(bundles)
        items.append(pattern_file)
        for b in items:
            existing = self.workpath / b.name
            if existing.exists():
                continue
            with (self.workpath / b.name).open('wb') as ofh:
                with b.open('rb') as fh:
                    ofh.write(fh.read())

    #def __del__(self):
    #    self.workdir.cleanup()
