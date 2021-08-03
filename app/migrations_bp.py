from flask import Blueprint

from app.container import container

migrations_bp = Blueprint('migrations', __name__)


@migrations_bp.post('/migrate/idms/mysql')
def migrate_idms_to_mysql():
    return container.idms_to_mysql_migration_service.migrate()
