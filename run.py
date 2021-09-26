import os

from app import app

if __name__ == '__main__':
    app.run(port=int(os.environ.get('PORT')))
