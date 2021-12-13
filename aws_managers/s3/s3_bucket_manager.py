from typing import List

from boto3 import client

from aws_managers.s3.s3_container_mixin import S3ContainerMixin
from aws_managers.s3.s3_folder_manager import S3FolderManager
from aws_managers.s3.utils import s3_bucket_uri


class S3BucketManager(
    S3ContainerMixin,
    object
):

    def __init__(self, name: str):
        """
        Create a new S3BucketManager.

        :param name: Name of the bucket.
        """
        self._bucket_name: str = name
        self._bucket_uri = s3_bucket_uri(name)
        self.prefix = None
        self._client = client('s3')

    @property
    def name(self) -> str:

        return self._bucket_name

    @property
    def uri(self) -> str:
        """
        Return the S3 URI of the bucket.
        """
        return self._bucket_uri

    def folders(self, deep: bool = False) -> List[S3FolderManager]:
        """
        Return an S3FolderManager reference to each sub-folder in the folder.
        """
        return [
            S3FolderManager(self._bucket_name, folder_key)
            for folder_key in self.folder_keys(deep=deep)
        ]

    def __truediv__(self, folder: str):

        return S3FolderManager(self.name, folder)
