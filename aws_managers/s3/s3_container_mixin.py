from botocore.client import BaseClient
from typing import List, Optional


class S3ContainerMixin(object):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """
    _bucket_name: str
    _bucket_uri: str
    prefix: Optional[str]
    uri: str
    _client: BaseClient

    def list_objects(self) -> List[dict]:
        """
        Returns a list of dicts describing each object in the container.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        """
        objects = []
        kwargs = dict(Bucket=self._bucket_name)
        if self.prefix is not None:
            kwargs['Prefix'] = self.prefix
        response: dict = self._client.list_objects_v2(**kwargs)
        if 'Contents' in response.keys():
            objects.extend([obj for obj in response['Contents']])
            while response['IsTruncated'] is True:
                continuation_token = response['NextContinuationToken']
                response: dict = self._client.list_objects_v2(
                    ContinuationToken=continuation_token,
                    **kwargs
                )
                objects.extend([obj for obj in response['Contents']])
        return objects

    def object_keys(self) -> List[str]:
        """
        Returns the value of 'Key' for each object (folder or file) in the
        container.
        """
        return [obj['Key'] for obj in self.list_objects()]

    def folder_keys(self, deep: bool = False) -> List[str]:
        """
        Return the name of each folder in the container.
        """
        if self.prefix is None:
            slashes = 0
        else:
            slashes = self.prefix.count('/')
        folder_keys = []
        for object_key in self.object_keys():
            if object_key.endswith('/'):
                if deep or object_key.count('/') == slashes + 1:
                    folder_keys.append(object_key)
        return folder_keys

    def folder_uris(self, deep: bool = False) -> List[str]:
        """
        Return the uri of each folder in the container.
        """
        return [
            f'{self._bucket_uri}{folder_key}'
            for folder_key in self.folder_keys(deep=deep)
        ]

    def file_keys(self, deep: bool = False) -> List[str]:
        """
        Return the name of each file in the container.
        """
        if self.prefix is None:
            slashes = 0
        else:
            slashes = self.prefix.count('/')
        file_keys = []
        for object_key in self.object_keys():
            if not object_key.endswith('/'):
                if deep or object_key.count('/') == slashes:
                    file_keys.append(object_key)
        return file_keys

    def file_uris(self, deep: bool = False) -> List[str]:
        """
        Return the uri of each folder in the container.
        """
        return [
            f'{self._bucket_uri}{file_key}'
            for file_key in self.file_keys(deep=deep)
        ]

    def size(self) -> int:
        """
        Return the size of the bucket and its contents, in bytes.
        """
        return sum([obj['Size'] for obj in self.list_objects()])

    def __repr__(self):

        return self.uri
