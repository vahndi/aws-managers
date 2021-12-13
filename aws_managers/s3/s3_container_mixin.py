from botocore.client import BaseClient
from typing import List, Optional


class S3ContainerMixin(object):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """
    _bucket_name: str
    _bucket_uri: str
    prefix: Optional[str]
    _client: BaseClient

    def list_object_keys(self) -> List[str]:
        """
        Returns the value of 'Key' for each item in the container.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        """
        object_keys = []
        kwargs = dict(Bucket=self._bucket_name)
        if self.prefix is not None:
            kwargs['Prefix'] = self.prefix
        response: dict = self._client.list_objects_v2(**kwargs)
        if 'Contents' in response.keys():
            object_keys.extend([f['Key'] for f in response['Contents']])
            while response['IsTruncated'] is True:
                continuation_token = response['NextContinuationToken']
                response: dict = self._client.list_objects_v2(
                    ContinuationToken=continuation_token,
                    **kwargs
                )
                object_keys.extend([f['Key'] for f in response['Contents']])
        return object_keys

    def folder_uris(self, deep: bool = False) -> List[str]:
        """
        Return the name of each folder in the container.
        """
        prefix_len = len(self.prefix)
        folder_uris = []
        for object_key in self.list_object_keys():
            if object_key.endswith('/'):
                if deep or object_key.count('/') == prefix_len + 1:
                    folder_uris.append(object_key)
        return folder_uris

    def file_uris(self, deep: bool = False) -> List[str]:
        """
        Return the name of each file in the container.
        """
        prefix_len = len(self.prefix)
        file_uris = []
        for object_key in self.list_object_keys():
            if not object_key.endswith('/'):
                if deep or object_key.count('/') == prefix_len:
                    file_uris.append(object_key)
        return file_uris

    def size(self) -> int:
        """
        Return the size of the bucket and its contents, in bytes.
        """
        raise NotImplementedError
