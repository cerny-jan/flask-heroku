import requests
import math


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

    def get_calls_from_api(self, start_date, end_date, company_id):
        """ Public method to pull calls from API in defined time range

        start_date: A string in format %Y-%m-%d
        end_date: A string in format %Y-%m-%d
        company_id: A string with company id (one account can have multiple companies in it)
        """
        additional_fields = [
            'formatted_customer_phone_number',
            'formatted_tracking_phone_number',
            'formatted_business_phone_number',
            'total_calls',
            'referrer_domain',
            'referring_url',
            'created_at',
            'utmv',
            'last_requested_url',
            'formatted_customer_name_or_phone_number',
            'waveforms',
            'formatted_duration',
            'note',
            'speaker_percent',
            'ga',
            'utm_term',
            'lead_status',
            'formatted_tracking_source',
            'medium',
            'company_id',
            'formatted_value',
            'utm_content',
            'call_highlights',
            'prior_calls',
            'utma',
            'utmb',
            'keywords',
            'utm_source',
            'formatted_customer_location',
            'tracker_id',
            'utm_medium',
            'good_lead_call_time',
            'source_name',
            'value',
            'device_type',
            'utmz',
            'good_lead_call_id',
            'company_time_zone',
            'keywords_spotted',
            'company_name',
            'utm_campaign',
            'first_call',
            'formatted_customer_name',
            'utmc',
            'tags',
            'landing_page_url',
            'gclid'
        ]

        payload = {
            'start_date': start_date,
            'end_date': end_date,
            'fields': ','.join(additional_fields),
            'company_id': company_id,
            'per_page': '250',
            'page': 0
        }
        result = self._make_api_request(payload, 'calls', page=1)
        self.logger.info('Pulled {} calls from CallRail API for company ID: {}, date range:  {} to {}. Needed {} request{}.'.format(
            len(result), company_id, start_date, end_date, math.ceil(len(result) / 250) if result else 1, 's' if len(result) > 1 else ''))
        return result

    def get_forms_from_api(self, company_id):
        """ Public method to pull form form submissions from API. This endpoint doesn't support date filtering.

            company_id: A string with company id (one account can have multiple companies in it)

        """
        payload = {
            'company_id': company_id,
            'per_page': '250',
            'page': 0
        }
        result = self._make_api_request(
            payload, 'form_submissions', page=1)
        self.logger.info('Pulled {} forms from CallRail API for company ID: {}. Needed {} request{}.'.format(
            len(result), company_id, math.ceil(len(result) / 250) if result else 1, 's' if len(result) > 1 else ''))
        return result

    def _make_api_request(self, payload, endpoint, page=1):
        """ Private method to make request to a specified endpoint

        payload: refer to https://apidocs.callrail.com/#api based on specified endpoint
        endpoint: calls or form_submissions or ...refer to https://apidocs.callrail.com/#api
        page (optional): default is 1, API uses pagination and can't return more than 250 calls
        per page http://apidocs.callrail.com/#pagination
        """
        payload['page'] = page
        api_url = 'https://api.callrail.com/v2/a/'
        headers = {
            'authorization': 'Token token={}'.format(self.callrail_token)
        }
        url = api_url + self.callrail_account_id + '/' + endpoint + '.json'
        try:
            r = requests.get(url, params=payload, headers=headers)
            response = r.json()
            if r.status_code == requests.codes.ok:
                result = response[endpoint]
                if response['page'] < response['total_pages']:
                    result += self._make_api_request(
                        payload, endpoint, page + 1)
                    return result
                else:
                    return result
            else:
                self.logger.error('{} Error: {}'.format(
                    r.status_code, response.get('error')))
                return ''
        except Exception as e:
            self.logger.error(str(e))
            return ''
