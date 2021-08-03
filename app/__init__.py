from flask import Flask

from app.migrations_bp import migrations_bp

app = Flask(__name__)
app.register_blueprint(migrations_bp)
