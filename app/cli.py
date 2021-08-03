import logging
from datetime import datetime

from app.config import DEBUG


def get_timestamp_block():
    return f'[{datetime.now()}]'


def log(data, level=logging.INFO):
    if level == logging.DEBUG and DEBUG:
        # Debug
        print(f'{get_timestamp_block()} {data}')
    elif level == logging.WARNING or level == logging.WARN:
        # Warning
        print(f'{get_timestamp_block()} 🟡 WARNING: {data}')
    elif level == logging.ERROR or level == logging.CRITICAL:
        # Error
        print(f'{get_timestamp_block()} ⛔ ERROR: {data}')
    elif level == logging.INFO:
        # Info
        print(f'{get_timestamp_block()} {data}')
