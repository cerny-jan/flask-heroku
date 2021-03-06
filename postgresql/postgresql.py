import os
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse as urlparse
from flask import flash
import datetime

# postgresql setup
db_url = urlparse.urlparse(os.environ['DATABASE_URL'])
conn = psycopg2.connect(
    database=db_url.path[1:],
    user=db_url.username,
    password=db_url.password,
    host=db_url.hostname,
    port=db_url.port
)


def insert_acitivity_list(activity_list):
    sql = """
    INSERT INTO activities(id,date,date_local,distance,duration,gpx,kilojoules,latitude_median,longitude_median, type, user_id)
    VALUES (%(id)s, %(date)s, %(date_local)s, %(distance)s, %(duration)s,%(gpx)s,
    %(kilojoules)s, %(latitude_median)s, %(longitude_median)s, %(type)s, %(user_id)s);
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, activity_list)
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(e)


def get_list_of_users():
    sql = """
    SELECT * FROM users;
     """
    users = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        users = cur.fetchall()
    except Exception as e:
        conn.rollback()
        flash('Database error', 'danger')
    return users


def get_users_latest_activity_date(user_id):
    sql = """
    SELECT date FROM activities WHERE user_id  = %s ORDER BY date DESC LIMIT 1;
    """
    latest_activity_date = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (user_id,))
        latest_activity_date = cur.fetchall()
    except Exception as e:
        conn.rollback()
    return latest_activity_date


def get_user_activities(userid, date_start, date_end):
    sql = """
    SELECT  date, gpx, latitude_median, longitude_median,  distance, type, user_id FROM activities WHERE user_id = %s AND to_date(date,'YYY-MM-DD') BETWEEN  %s AND  %s;
    """
    user_activities = []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (userid, date_start, date_end,))
        user_activities = cur.fetchall()
    except Exception as e:
        conn.rollback()
        user_activities = ['Database error']
    return user_activities


def create_user_if_does_not_exit(user_token):
    sql = """
    INSERT INTO users values (%s, %s,%s) ON CONFLICT DO NOTHING;
    """
    user_id = user_token.get('athlete', {}).get('id')
    firstname = user_token.get('athlete', {}).get('firstname')
    lastname = user_token.get('athlete', {}).get('lastname')
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (user_id, firstname, lastname,))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
