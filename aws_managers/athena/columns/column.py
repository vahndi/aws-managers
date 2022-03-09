from aws_managers.athena.functions.aggregate import CountMixin


class Column(
    CountMixin,
    object
):

    def __init__(self, name: str):

        self.name: str = name

    def __str__(self):

        return self.name
