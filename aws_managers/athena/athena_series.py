from typing import Optional, Tuple, Union

from awswrangler.athena import read_sql_query
from pandas import Series, DataFrame

from aws_managers.athena.queries.athena_query_generator import AthenaQueryGenerator
from aws_managers.athena.clauses.conjunctive_operators import \
    ConjunctiveOperator
from aws_managers.athena.operators.mixins import ComparisonMixin


class AthenaSeries(object):

    def __init__(
            self,
            database: str,
            table: str,
            column: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None,
            column_info: Optional[Series] = None,
    ):
        """
        Create a new AthenaFrame.

        :param database: Name of the Athena database.
        :param table: Name of the Athena table.
        :param column: Name of the column.
        :param sample: Optional tuple of 'BERNOULLI' or 'SYSTEM' and an
                       integer percentage.
        :param column_info: Column info from schema if this is a subset of an
                            existing frame. Leave as None for a new Series.
        """
        self._q: AthenaQueryGenerator = AthenaQueryGenerator()
        self._database: str = database
        self._table: str = table
        self._column: str = column
        if isinstance(column_info, Series):
            self._column_info: Series = column_info
        else:
            column_info: DataFrame = self._execute(
                sql=self._q.column_info(
                    database=self._database, table=self._table)
            )
            self._column_info: Series = column_info.loc[
                column_info['column_name'] == self._column
            ].iloc[0]
        self._sample: Optional[Tuple[str, int]] = sample
        self._where = where

    def _execute(self, sql: str) -> DataFrame:
        """
        Execute a query.

        :param sql: Raw SQL to execute.
        """
        data = read_sql_query(sql=sql, database=self._database)
        return data
