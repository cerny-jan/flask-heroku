import os
from flask import Flask
import psycopg2
import urllib.parse as urlparse

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db_url = urlparse.urlparse(os.environ['DATABASE_URL'])
conn = psycopg2.connect(
    database=db_url.path[1:],
    user=db_url.username,
    password=db_url.password,
    host=db_url.hostname,
    port=db_url.port
)
cur = conn.cursor()


@app.route('/')
def index():
    cur.execute("SELECT * FROM test;")
    result = cur.fetchone()
    return result[1]


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
