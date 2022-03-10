from typing import Any


class ComparisonMixin(object):
    """
    https://prestodb.io/docs/current/functions/comparison.html
    """
    def __init__(self, column: str, operator: str, value):

        self.column: str = column
        self.operator: str = operator
        self.value: Any = value
