from app.idms_to_mysql_migration.service import IDMSToMySQLMigrationService


class __Container:
    """Container for dependencies."""

    def __init__(self):
        self.idms_to_mysql_migration_service = IDMSToMySQLMigrationService()


container = __Container()
