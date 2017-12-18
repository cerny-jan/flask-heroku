import requests
from datetime import date, timedelta


class CallRail:

    def __init__(self, logger, callrail_account_id, callrail_token):
        """ Intialise CallRail client

        callrail_account_id: A string representing your CallRail account ID
        callrail_token: A string representing your secret CallRail token
        """
        self.__callrail_account_id = callrail_account_id
        self.__callrail_token = callrail_token
        self.__logger = logger

    @property
    def callrail_account_id(self):
        return self.__callrail_account_id

    @property
    def callrail_token(self):
        return self.__callrail_token

    @property
    def logger(self):
        return self.__logger

    def get_calls_from_api(self, start_date, end_date, company_id, page=1):
        """ Public method to pull calls from API in defined time range

        start_date: A string in format %Y-%m-%d
        end_date: A string in format %Y-%m-%d
        company_id: A string with company id (one account can have multiple companies in it)
        page (optional): default is 1, API uses pagination and can't return more than 250 calls per request http://apidocs.callrail.com/#pagination
        """
        api_url = 'https://api.callrail.com/v2/a/'
        headers = {
            'authorization': 'Token token={}'.format(self.callrail_token)
        }
        payload = {
            'start_date': start_date,
            'end_date': end_date,
            'fields': 'keywords,landing_page_url,gclid',
            'company_id': company_id,
            'per_page': '250',
            'page': page
        }
        url = api_url + self.callrail_account_id + '/calls.json'
        try:
            r = requests.get(url, params=payload, headers=headers)
            response = r.json()
            if response['page'] == response['total_pages']:
                self.logger.info('Pulled calls from CallRail API for company ID: {}, date range:  {} to {}. Needed {} request{}.'.format(
                    company_id, start_date, end_date, response['total_pages'], 's' if response['total_pages'] > 1 else ''))
            if response['page'] < response['total_pages']:
                result = response['calls'] + self.get_calls_from_api(
                    start_date, end_date, company_id, page + 1)
                return result
            else:
                return response['calls']
        except Exception as e:
            self.logger.error(str(e))
