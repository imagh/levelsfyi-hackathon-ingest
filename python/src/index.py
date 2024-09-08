from flask import Flask
from psycopg2 import connect

app = Flask(__name__)
pool = connect()

@app.route("/")
def index():
  with pool.cursor() as cursor:
    cursor.execute("SELECT 5 * 5 AS value")
    return dict(result=cursor.fetchone()[0])
