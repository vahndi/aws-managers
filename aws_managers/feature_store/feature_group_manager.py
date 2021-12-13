from typing import List, Dict

from boto3 import client
from pandas import DataFrame, read_parquet


class FeatureGroupManager(object):
    """
    Class with some convenience methods wrapping the boto3 sagemaker client and
    pandas read methods.
    """
    _client = client('sagemaker')

    def __init__(self, group_name: str):
        """
        Create a new FeatureGroupManager.

        :param group_name: Name of the FeatureGroup.
        """
        self._group_name: str = group_name
        self._describe = self._client.describe_feature_group(
            FeatureGroupName=group_name)

    @staticmethod
    def feature_group_names() -> List[str]:
        """
        Return a list of all the FeatureGroups in the FeatureStore.
        """
        return [
            feature_group['FeatureGroupName']
            for feature_group in
            FeatureGroupManager._client.list_feature_groups()[
                'FeatureGroupSummaries']
        ]

    @property
    def group_name(self) -> str:
        """
        Return the FeatureGroup name.
        """
        return self._group_name

    @property
    def description(self) -> dict:
        """
        Return the FeatureGroup description.
        """
        return self._describe['Description']

    @property
    def feature_definitions(self) -> List[Dict[str, str]]:
        """
        Return a list of feature definition dicts in the FeatureGroup with the
        given name.
        """
        return self._describe['FeatureDefinitions']

    @property
    def feature_names(self) -> List[str]:
        """
        Return a list of the names of the features in the FeatureGroup with the
        given name.
        """
        return [definition['FeatureName'] for definition in
                self.feature_definitions]

    @property
    def record_identifier_feature_name(self) -> str:
        """
        Return the name of the record identifier used in creation of the group.
        """
        return self._describe['RecordIdentifierFeatureName']

    @property
    def s3_uri__offline_store(self) -> str:
        return self._describe['OfflineStoreConfig']['S3StorageConfig']['S3Uri']

    @property
    def s3_uri__offline_store__resolved_output(self) -> str:
        return self._describe['OfflineStoreConfig']['S3StorageConfig'][
            'ResolvedOutputS3Uri']

    def read_offline_data(self, drop_metadata: bool = True,
                          **read_parquet_kwargs) -> DataFrame:
        """
        Read the dataset from the offline store.

        :param drop_metadata: If True, drop columns created by FeatureStore.
        :param read_parquet_kwargs: kwargs to pass to pandas.read_parquet
        """
        data = read_parquet(path=self.s3_uri__offline_store__resolved_output,
                            **read_parquet_kwargs)
        data = data.set_index(self.record_identifier_feature_name)
        if drop_metadata:
            metadata_cols = ['EventTime', 'write_time', 'api_invocation_time',
                             'is_deleted', 'year', 'month', 'day', 'hour']
            data = data.drop(metadata_cols, axis=1)
        return data

    def delete_group(self):
        """
        Delete the FeatureGroup.
        """
        self._client.delete_feature_group(FeatureGroupName=self._group_name)