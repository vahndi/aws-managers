from typing import Any


class ComparisonMixin(object):
    """
    https://prestodb.io/docs/current/functions/comparison.html
    """
    def __init__(self, column: str, operator: str, value):

        self.column: str = column
        self.operator: str = operator
        self.value: Any = value


class ScalarComparison(ComparisonMixin, object):

    def __str__(self):

        return f'{self.column} {self.operator} {self.value}'


class TimestampComparison(ComparisonMixin, object):

    def __str__(self):

        return f'{self.column} {self.operator} timestamp {self.value}'


class StringComparison(ComparisonMixin, object):

    def __str__(self):

        return f"{self.column} {self.operator} '{self.value}'"
