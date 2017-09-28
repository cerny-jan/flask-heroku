import requests
import arrow


class Strava:
    API_BASE_URL = 'https://www.strava.com/api/v3'

    def __init__(self, access_token=None, ratelimit_limit=None, ratelimit_usage=None):
        '''
        :param access_token: The token that allows access user's data
        :type access_token: str
        '''
        self.access_token = access_token
        # TO DO: rate limit
        self.ratelimit_limit = ratelimit_limit
        self.ratelimit_usage = ratelimit_usage

    def get_athlete(self):
        return self._get_request('/athlete')

    def get_activities(self, before=None, after=None, per_page=200):
        if before:
            before = arrow.get(before).timestamp
        elif after:
            after = arrow.get(after).timestamp
        params = dict(before=before,
                      after=after,
                      per_page=per_page,
                      page=1
                      )
        result = []
        while True:
            activities = self._get_request('/activities', **params)
            if not activities:
                break
            else:
                result += activities
                params['page'] += 1
        return result

    def get_simplified_activities(self, before=None, after=None):
        simplified_activities = []
        raw_activities = self.get_activities(before, after)
        for raw_activity in raw_activities:
            simplified_activities.append(
                {
                    'activity_id': raw_activity.get('id'),
                    'distance': raw_activity.get('distance'),
                    'moving_time': raw_activity.get('moving_time'),
                    'type': raw_activity.get('type'),
                    'start_date': raw_activity.get('start_date'),
                    'start_date_local': raw_activity.get('start_date_local'),
                    'kilojoules': raw_activity.get('kilojoules')
                }
            )
        return simplified_activities

    def get_stream(self, activity_id):
        streams = self._get_request(
            '/activities/' + str(activity_id) + '/streams/latlng')
        for stream in streams:
            if stream.get('type') == 'latlng':
                return stream.get('data')

    def _get_request(self, url, **kwargs):
        query_params = {'access_token': self.access_token}
        for name, value in kwargs.items():
            query_params[name] = value

        response = requests.get(self.API_BASE_URL + url,
                                params=query_params)
        self.ratelimit_limit = response.headers.get('X-RateLimit-Limit')
        self.ratelimit_usage = response.headers.get('X-RateLimit-Usage')
        if response.status_code != 200:
            response.raise_for_status()
        else:
            return response.json()
