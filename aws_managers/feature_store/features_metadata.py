from pathlib import Path
from typing import Optional, Dict, List

from pandas import DataFrame, read_excel, Series, isnull

from aws_managers.utils.dtype_mappings import FS_NAME_TO_ATHENA_NAME


class FeaturesMetadata(object):

    def __init__(self, metadata_fn: Path, dataset_name: str):
        """
        Class to rename columns

        :param dataset_name: Name of the dataset in the metadata spreadsheet.
        """
        # read metadata
        self.columns: DataFrame = read_excel(
            metadata_fn,
            sheet_name=dataset_name, engine='openpyxl'
        ).set_index('original_name')
        attributes = read_excel(
            metadata_fn,
            sheet_name='attributes', engine='openpyxl'
        )
        self.attributes: Series = attributes.loc[
            attributes['dataset'] == dataset_name
            ].set_index('attribute_name')['attribute_type']
        self._name_mapping: Optional[Dict[str, str]] = None

    def check_d_types(self, data: DataFrame):
        """
        Check that the data can be converted to the d-types in the metadata.

        :param data: The data whose d-types to check.
        """
        for old_name, attribute_values in self.columns.iterrows():
            print(f'\rChecking d-type for column {old_name}' + ' ' * 256,
                  end='')
            if attribute_values['data_type'] == 'Integral':
                _ = data[old_name].dropna().astype(int)
            elif attribute_values['data_type'] == 'Fractional':
                _ = data[old_name].dropna().astype(float)
            elif attribute_values['data_type'] == 'String':
                _ = data[old_name].dropna().astype('string')
        print('\nAll checks passed.')

    @property
    def name_mapping(self) -> Dict[str, str]:
        """
        Return a dictionary that maps old feature names to new ones.
        """
        # return mapping if it already exists
        if self._name_mapping is not None:
            return self._name_mapping
        # build mapping
        old_to_new_name = {}
        old_name: str
        for old_name, attr_values in self.columns.iterrows():
            new_name = f"{attr_values['feature']}___{attr_values['metric']}"
            for attr_name, attr_type in self.attributes.items():
                attribute_value = self.columns.loc[old_name, attr_name]
                if isnull(attribute_value):
                    continue
                if attr_type == 'string':
                    new_name += f'___{attr_name}__{attribute_value}'
                elif attr_type == 'bool':
                    if attribute_value == True:
                        new_name += f'___{attr_name}'
                    elif attribute_value == False:
                        new_name += f'___not_{attr_name}'
                    else:
                        raise ValueError(
                            f'{attr_name} should be equal to True or False '
                            f'but is {attribute_value}'
                        )
                elif attr_type == 'int_range':
                    new_name += f'___{attr_name}__{attribute_value}'
                else:
                    raise ValueError(
                        f'Invalid attribute type for attribute '
                        f'{attr_name} ({attr_type})'
                    )
            # set mapping
            old_to_new_name[old_name] = new_name
        # return created  mapping
        self._name_mapping = old_to_new_name
        return self._name_mapping

    @property
    def old_names(self) -> List[str]:
        """
        Return the old names of the dataset, as listed in the metadata.
        """
        return self.columns.index.to_list()

    @property
    def new_names(self) -> List[str]:
        """
        Return the old names of the dataset, as listed in the metadata.
        """
        mapping = self.name_mapping
        return [mapping[old_name] for old_name in self.old_names]

    @property
    def feature_types(self) -> Dict[str, str]:
        """
        Return a dictionary that maps new feature names to their types.
        """
        mapping = self.name_mapping
        return {
            mapping[old_name]: data_type
            for old_name, data_type in self.columns['data_type'].items()
        }

    def athena_schema(self, identifier_name: str, identifier_type: str) -> str:
        """
        Return a string of pairs of new column name and Athena data type.

        :param identifier_name: Name of the FeatureStore record identifier.
        :param identifier_type: Data type of the FeatureStore record identifier.
                                One of {'String', 'Integral', 'Fractional'}
        """
        str_out = (
            f'{identifier_name} {FS_NAME_TO_ATHENA_NAME[identifier_type]},\n'
        )
        mapping = self.name_mapping
        str_out += ',\n'.join([
            f'{mapping[old_name]} {FS_NAME_TO_ATHENA_NAME[data_type]}'
            for old_name, data_type in self.columns['data_type'].items()
        ]) + '\n'
        return str_out
