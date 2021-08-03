from os import environ
from dotenv import load_dotenv

load_dotenv()

__debug = environ.get('DEBUG')
DEBUG = bool(int(__debug)) if __debug is not None else False

S3_EVE_BUCKET = environ.get('S3_EVE_BUCKET')
S3_THEORY_BUCKET = environ.get('S3_THEORY_BUCKET')
