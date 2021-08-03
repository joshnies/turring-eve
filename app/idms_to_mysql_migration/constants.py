from app.idms_to_mysql_migration.mysql_column import MySQLColumn

IDMS_TO_MYSQL_TYPE_MAP = {
    'A': 'CHAR',
    'X': 'CHAR',
    '9': 'NUMERIC'
}

MYSQL_ID_COLUMN = MySQLColumn(
    name='id',
    var_type='CHAR',
    length=9
)
