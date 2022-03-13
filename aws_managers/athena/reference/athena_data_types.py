"""
https://docs.aws.amazon.com/athena/latest/ug/data-types.html
"""
ATHENA_BOOLEAN_TYPES = [
    'boolean'
]
ATHENA_INTEGER_TYPES = [
    'tinyint',
    'smallint',
    'integer',
    'bigint'
]
ATHENA_REAL_TYPES = [
    'float',
    'double',
    'decimal',
    'real'  # real is Presto's equivalent of float
]
ATHENA_DATETIME_TYPES = [
    'date',
    'timestamp'
]
ATHENA_CHARACTER_TYPES = [
    'char',
    'varchar',
    'string'
]
ATHENA_NUMERIC_TYPES = (
    ATHENA_BOOLEAN_TYPES +
    ATHENA_INTEGER_TYPES +
    ATHENA_REAL_TYPES
)
