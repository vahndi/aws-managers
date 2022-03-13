from aws_managers.athena.queries.column_query import ColumnQuery
from aws_managers.athena.functions.aggregate import MaxMixin, MinMixin
from aws_managers.athena.operators.comparisons import TimestampComparison


class TimestampColumnQuery(
    MaxMixin,
    MinMixin,
    ColumnQuery
):

    @staticmethod
    def value(year: int, month: int, day: int,
              hour: int = 0, minute: int = 0, second: float = 0.0) -> str:
        return (
            f"'{year}-{month:02d}-{day:02d} "
            f"{hour:02d}:{minute:02d}:{second:06.3f}'"
        )

    def __eq__(self, other: str) -> TimestampComparison:
        return TimestampComparison(self.name, '=', other)

    def __ne__(self, other) -> TimestampComparison:
        return TimestampComparison(self.name, '<>', other)

    def __lt__(self, other: str) -> TimestampComparison:
        return TimestampComparison(self.name, '<', other)

    def __gt__(self, other: str) -> TimestampComparison:
        return TimestampComparison(self.name, '>', other)

    def __le__(self, other: str) -> TimestampComparison:
        return TimestampComparison(self.name, '<=', other)

    def __ge__(self, other: str) -> TimestampComparison:
        return TimestampComparison(self.name, '>=', other)
