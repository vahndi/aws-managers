from typing import List

from aws_managers.s3.s3_container_mixin import S3ContainerMixin
from aws_managers.s3.utils import s3_bucket_uri


class S3FolderManager(
    S3ContainerMixin,
    object
):

    def __init__(self, bucket_name: str, *folder_path: str):

        if len(folder_path) == 0:
            raise ValueError(
                'Must provide at least one folder in the path'
            )
        self._bucket_name: str = bucket_name
        folder_names = []
        for component in folder_path:
            folder_names.extend([
                f for f in component.split('/')
                if f != ''
            ])
        self._folder_path: List[str] = folder_names
        self._name = folder_path[-1]

    def __truediv__(self, folder: str):

        return S3FolderManager(self._bucket_name, *self._folder_path, folder)

    @property
    def name(self):
        return self._name

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def parent(self) -> 'S3FolderManager':

        if len(self._folder_path) == 0:
            raise ValueError(
                f'The parent of folder "{self.uri}" is '
                f'the bucket "{self._bucket_name}".'
            )
        return S3FolderManager(self._bucket_name, *self._folder_path[: -1])

    @property
    def folder_path(self) -> List[str]:

        return self._folder_path

    @property
    def prefix(self) -> str:

        return '/'.join(self._folder_path) + '/'

    @property
    def uri(self) -> str:
        """
        Return the URI of the folder.
        """
        return s3_bucket_uri(self._bucket_name) + (
            '/'.join(self._folder_path)
        ) + '/'
