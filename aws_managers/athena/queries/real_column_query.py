from aws_managers.athena.queries.column_query import ColumnQuery
from aws_managers.athena.functions.aggregate import AvgMixin, \
    GeometricMeanMixin, MaxMixin, MinMixin, SumMixin


class RealColumnQuery(
    AvgMixin,
    GeometricMeanMixin,
    MaxMixin,
    MinMixin,
    SumMixin,
    ColumnQuery
):

    pass
