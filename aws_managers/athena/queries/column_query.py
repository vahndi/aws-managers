from aws_managers.athena.functions.aggregate import CountMixin
from aws_managers.athena.functions.window import LagMixin


class ColumnQuery(
    CountMixin,
    LagMixin,
    object
):

    def __init__(self, name: str):

        self.name: str = name

    def __str__(self):

        return self.name
