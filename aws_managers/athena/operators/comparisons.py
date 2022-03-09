from aws_managers.athena.operators.mixins import ComparisonMixin


class ScalarComparison(ComparisonMixin, object):

    def __str__(self):

        return f'{self.column} {self.operator} {self.value}'


class TimestampComparison(ComparisonMixin, object):

    def __str__(self):

        return f'{self.column} {self.operator} timestamp {self.value}'


class StringComparison(ComparisonMixin, object):

    def __str__(self):

        return f"{self.column} {self.operator} '{self.value}'"
