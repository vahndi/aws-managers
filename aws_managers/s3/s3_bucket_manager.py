from boto3 import client

from aws_managers.s3.s3_container_mixin import S3ContainerMixin
# get_execution_role()
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
        self.name: str = name
        self._bucket_uri = s3_bucket_uri(name)
        self.prefix = None
        self._client = client('s3')

    @property
    def uri(self) -> str:
        """
        Return the S3 URI of the bucket.
        """
        return self._bucket_uri

    def __truediv__(self, folder: str):

        return S3FolderManager(self.name, folder)

