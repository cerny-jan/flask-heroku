import os
from flask import Flask, render_template, request, jsonify, flash, session, redirect, url_for
from flask_compress import Compress
from requests_oauthlib import OAuth2Session
from strava.strava import Strava
import pandas as pd
import numpy as np
from celery import Celery
import datetime
from postgresql.postgresql import *


app = Flask(__name__)
app.url_map.strict_slashes = False
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = os.environ['FLASK_SECRET']
Compress(app)


# Strava API setup
client_id = os.environ['STRAVA_ID']
client_secret = os.environ['STRAVA_SECRET']
authorization_base_url = 'https://www.strava.com/oauth/authorize'
token_url = 'https://www.strava.com/oauth/token'
redirect_uri = os.environ['REDIRECT_URI']


celery = Celery(app.name)
celery.conf.update(
    BROKER_URL=os.environ['REDIS_URL'], CELERY_RESULT_BACKEND=os.environ['REDIS_URL'])


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
    users = get_list_of_users()
    users = [u for u in users]
    for user in users:
        if str(user['id']) == user_id_from_cookie:
            current_user = user
            break
    else:
        current_user = users[0] if users else None
    return render_template('activity-dashboard.html', is_dev=app.debug, users=users, current_user=current_user)


@app.route('/activity-dashboard/update')
def login():
    strava_auth = OAuth2Session(client_id, redirect_uri=redirect_uri)
    authorization_url, state = strava_auth.authorization_url(
        authorization_base_url)
    session['oauth_state'] = state
    return redirect(authorization_url)


@app.route('/activity-dashboard/callback')
def callback():
    strava_auth = OAuth2Session(client_id, state=session['oauth_state'])
    token = strava_auth.fetch_token(
        token_url, client_secret=client_secret, authorization_response=request.url)

    create_user_if_does_not_exit(token)
    access_token = token.get('access_token')
    user_id = token.get('athlete', {}).get('id')
    latest_activity_date = get_users_latest_activity_date(user_id)
    latest_activity_date = latest_activity_date[0].get(
        'date') if latest_activity_date else '2017-01-01'
    # latest_activity_date = '2017-09-28'
    strava = Strava(access_token)
    activities = strava.get_simplified_activities(after=latest_activity_date)
    response = app.make_response(redirect(url_for('activity_dashboard')))
    number_of_activities = len(activities)
    if number_of_activities == 0:
        flash('No new activity', 'info')
    else:
        response.set_cookie('user', value=str(
            user_id), expires=datetime.datetime.now() + datetime.timedelta(days=90))
        if number_of_activities <= 8:
            load_data(token, latest_activity_date)
            if number_of_activities == 1:
                flash('1 activty loaded.', 'success')
            else:
                flash('{} activities loaded.'.format(
                    number_of_activities), 'success')
        else:
            task = load_data.apply_async((token, latest_activity_date))
            response.set_cookie('task_id', value=task.id)
            flash('Loading {} activities. This may take a while.'.format(
                number_of_activities), 'info')
    return response


@celery.task(bind=True)
def load_data(self, token, latest_date):
    try:
        access_token = token.get('access_token')
        strava = Strava(access_token)
        activities = strava.get_simplified_activities(after=latest_date)
        df = pd.DataFrame(activities)

        def get_gpx(x):
            def calculate_distance(lat1, lat2, long1, long2):
                lat1 = np.radians(lat1)
                lat2 = np.radians(lat2)
                long1 = np.radians(long1)
                long2 = np.radians(long2)
                # http://www.movable-type.co.uk/scripts/latlong.html
                R = 6371  # radius of the earth in km
                R = R * 1000  # converted to metres
                x = (long2 - long1) * np.cos(0.5 * (lat2 + lat1))
                y = lat2 - lat1
                return R * np.sqrt(x * x + y * y)

            def to_del(row):
                return row['dist'] < 8
            df = pd.DataFrame(strava.get_stream(
                x['activity_id']), columns=['lat', 'long'])
            df['dist'] = calculate_distance(
                df.lat.shift(), df.loc[1:, 'lat'], df.long.shift(), df.loc[1:, 'long'])
            df['del'] = df[::2].apply(to_del, axis=1)
            df['del'].fillna(False, inplace=True)
            result = df[~df['del']]
            result['dist'] = calculate_distance(result.lat.shift(
            ), result.loc[1:, 'lat'], result.long.shift(), result.loc[1:, 'long'])
            result = result[result['dist'] > 6]
            result.drop(['dist', 'del'], axis=1, inplace=True)
            return result.values.tolist()

        df['gpx'] = df.apply(get_gpx, axis=1)

        def latitude_median(row):
            return pd.DataFrame([gpx[0] for gpx in row]).median()

        def longitude_median(row):
            return pd.DataFrame([gpx[1] for gpx in row]).median()
        df['latitude_median'] = df['gpx'].apply(latitude_median)
        df['longitude_median'] = df['gpx'].apply(longitude_median)
        df['distance'] = (df['distance'] / 1000).round(2)
        df.rename(columns={'activity_id': 'id', 'moving_time': 'duration',
                           'start_date': 'date', 'start_date_local': 'date_local'}, inplace=True)
        df['user_id'] = token.get('athlete', {}).get('id')
        insert_acitivity_list(df.to_dict(orient='records'))
        return 'SUCCESS MESSAGE'
    except Exception as e:
        self.update_state(state='FAILURE', meta=str(e))
        raise Exception()


@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = load_data.AsyncResult(task_id)
    return str(task.status)


@app.route('/api/rk/<userid>')
def rk(userid):
    return jsonify(get_user_activities(userid))


@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html', is_dev=app.debug), 404


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
