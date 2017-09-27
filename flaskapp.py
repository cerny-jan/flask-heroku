import os
from flask import Flask, render_template, request, jsonify, flash
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse as urlparse
from flask_compress import Compress

app = Flask(__name__)
app.url_map.strict_slashes = False
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = os.environ['FLASK_SECRET']
Compress(app)


db_url = urlparse.urlparse(os.environ['DATABASE_URL'])
conn = psycopg2.connect(
    database=db_url.path[1:],
    user=db_url.username,
    password=db_url.password,
    host=db_url.hostname,
    port=db_url.port
)


@app.before_request
def clear_trailing():
    from flask import redirect, request

    rp = request.path
    if rp != '/' and rp.endswith('/'):
        return redirect(rp[:-1])


@app.route('/')
def index():
    return render_template('index.html', is_dev=app.debug)


@app.route('/activity-dashboard')
def activity_dashboard():
    user_id_from_cookie = request.cookies.get('user')
    users = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM users;")
        users = cur.fetchall()
    except Exception as e:
        conn.rollback()
        flash('Database error', 'danger')
    users = [u for u in users]
    for user in users:
        if str(user['id']) == user_id_from_cookie:
            current_user = user
            break
    else:
        current_user = users[0] if users else None
    return render_template('activity-dashboard.html', is_dev=app.debug, users=users, current_user=current_user)


@app.route('/api/rk/<userid>')
def rk(userid):
    sql = """
    SELECT  date, gpx, latitude_median, longitude_median,  distance, type, user_id FROM activities where user_id = %s;
    """
    user_activities = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (userid,))
        user_activities = cur.fetchall()
    except Exception as e:
        conn.rollback()
        user_activities = ['Database error']
    finally:
        return jsonify(user_activities)


@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html', is_dev=app.debug), 404


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
