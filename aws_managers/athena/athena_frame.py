from typing import Optional, Dict

from awswrangler.athena import read_sql_query
from pandas import DataFrame, Index, Series

from aws_managers.athena.athena_query_generator import AthenaQueryGenerator


class AthenaFrame(object):

    def __init__(
            self,
            database: str,
            table: str,
            column_info: Optional[DataFrame] = None
    ):
        """
        Create a new AthenaTableManager.

        :param database: Name of the Athena database.
        :param table: Name of the Athena table.
        :param column_info: Column info from schema if this is a subset of an
                            existing frame. Leave as None for a new Frame.
        """
        self._q: AthenaQueryGenerator = AthenaQueryGenerator()
        self._database: str = database
        self._table: str = table
        if isinstance(column_info, DataFrame):
            self.column_info: DataFrame = column_info
        else:
            self._column_info: DataFrame = self._execute(
                sql=self._q.column_info(
                    database=self._database, table=self._table)
                )

    def _execute(self, sql: str) -> DataFrame:
        """
        Execute a query.

        :param sql: Raw SQL to execute.
        """
        data = read_sql_query(sql=sql, database=self._database)
        return data

    @property
    def columns(self) -> Index:
        """
        The column labels of the table.
        """
        return Index(self._column_info['column_name'].to_list())

    @property
    def dtypes(self) -> Series:
        """
        Return the dtypes in the table.
        """
        return self._column_info.set_index('column_name')['data_type']

    def n_unique(self):
        """
        Count number of distinct elements.
        """
        data = self._execute(self._q.count_distinct(
            columns=self.columns.to_list(),
            database=self._database,
            table=self._table
        ))
        return data.iloc[0]
