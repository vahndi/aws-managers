from pandas import DataFrame

from aws_managers.athena.reference.athena_data_types import \
    ATHENA_BOOLEAN_TYPES, ATHENA_INTEGER_TYPES, ATHENA_REAL_TYPES, \
    ATHENA_CHARACTER_TYPES, ATHENA_DATETIME_TYPES
from aws_managers.athena.queries import \
    BooleanColumnQuery, IntegerColumnQuery, RealColumnQuery, \
    StringColumnQuery, TimestampColumnQuery


class AthenaColumnQuerySet(object):

    def __init__(self, column_info: DataFrame):
        """
        Create a new set of Athena columns for interactive querying.

        :param column_info: see AthenaQueryGenerator.column_info
        """
        self._column_info: DataFrame = column_info
        for _, row in column_info.iterrows():
            name = row['column_name']
            data_type = row['data_type']
            try:
                if data_type in ATHENA_BOOLEAN_TYPES:
                    setattr(self, name, BooleanColumnQuery(name))
                elif data_type in ATHENA_INTEGER_TYPES:
                    setattr(self, name, IntegerColumnQuery(name))
                elif data_type in ATHENA_REAL_TYPES:
                    setattr(self, name, RealColumnQuery(name))
                elif data_type in ATHENA_CHARACTER_TYPES:
                    setattr(self, name, StringColumnQuery(name))
                elif data_type in ATHENA_DATETIME_TYPES:
                    setattr(self, name, TimestampColumnQuery(name))
                else:
                    print(
                        f'Warning - no matching column for '
                        f'data-type {data_type}'
                    )
            except TypeError:
                print(
                    f'Warning - could not set dynamic property for '
                    f'{name}'
                )
