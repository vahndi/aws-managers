from pathlib import Path

DIR_PROJECT = Path(__file__)

while DIR_PROJECT.name != 'aws_managers':
    DIR_PROJECT = DIR_PROJECT.parent

DIR_TEMPLATES = DIR_PROJECT / 'templates'
DIR_ATHENA_TEMPLATES = DIR_TEMPLATES / 'athena'
