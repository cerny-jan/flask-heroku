import os
import requests
from pprint import pprint
import json
from datetime import date, timedelta
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud import logging as google_logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.oauth2 import service_account
import logging


class CallRail:
    """ Call Rail Class for interacting with Call Rail API and Google BigQuery

    Main Purpose of this is to get the data from Call Rail API to Google BigQuery
    """
    development = os.getenv('DEVELOPMENT', 0)

    def __init__(self, callrail_account_id, callrail_token, google_project_id, bq_dataset_id, google_servise_account_info=None, debug=False):
        """ Intialise CallRail client

        callrail_account_id: A string representing your CallRail account ID
        callrail_token: A string representing your secret callrail token
        google_project_id: A string representing Google project ID that should be used
        bq_dataset_id: A string representing Google BigQuery dataset ID that should be used
        google_servise_account_info (optional): A string of JSON object representing your google service JSON key, if none is specified, the app will try to use Application Default Credentials
                    https://developers.google.com/identity/protocols/application-default-credentials
        debug (opional): A boolean value that sets the app to debug mode -> disables logging to Google stackdriver
        """
        self.debug = debug
        self.callrail_account_id = callrail_account_id
        self.callrail_token = callrail_token
        self.dataset_id = bq_dataset_id
        self.__set_google_credentials(google_servise_account_info)
        self.__set_bigquery_client(google_project_id)
        self.__set_dataset_ref(bq_dataset_id)
        self.__set_logging(google_project_id, bq_dataset_id)

    def __set_google_credentials(self, google_service_account_info):
        """ Private method to set Google credentials

        It will use default Google application credentials if there is no google_service_account_info provided or app is running in dev mode
        """
        if self.development or not google_service_account_info:
            google_credentials = service_account.Credentials.from_service_account_file(
                os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
            self.google_credentials = google_credentials
        else:
            google_credentials = service_account.Credentials.from_service_account_info(
                json.loads(google_service_account_info))
            self.google_credentials = google_credentials

    def __set_bigquery_client(self, google_project_id):
        """ Private method to set Google BigQuery Client
        """
        bigquery_client = bigquery.Client(
            credentials=self.google_credentials, project=google_project_id)
        self.bigquery_client = bigquery_client

    def __set_dataset_ref(self, bq_dataset_id):
        dataset_ref = self.bigquery_client.dataset(bq_dataset_id)
        self.dataset_ref = dataset_ref

    def __set_logging(self, google_project_id, logger_name):
        """ Private method to set logging_client

        If debug mode is off, the app will log to console and Google stackdriver
        If debug mode is on, it will log only to console
        """
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        if not self.debug:
            # Instantiates a google logging client
            logging_client = google_logging.Client(
                credentials=self.google_credentials, project=google_project_id)
            # setup logging to google stackdriver
            google_handler = CloudLoggingHandler(
                logging_client, name=logger_name)
            logger.addHandler(google_handler)
        # setup logging to console
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)
        self.logger = logger

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
                self.logger.info('Calls for company ID: {} from {} to {} pulled from API. {} request{} needed.'.format(
                    company_id, start_date, end_date, response['total_pages'], 's were' if response['total_pages'] > 1 else ' was'))
            if response['page'] < response['total_pages']:
                result = response['calls'] + \
                    self.get_calls_from_api(
                        start_date, end_date, company_id, page + 1)
                return result
            else:
                return response['calls']
        except Exception as e:
            self.logger.error(str(e))

    def stream_data_to_bq(self, bq_table_id, json_data):
        """ Public method to stream data to bigquery

        Streamed data is available for real-time analysis within a few seconds of the first streaming insertion into a table. https://cloud.google.com/bigquery/streaming-data-into-bigquery
        There is a limit of 10k rows per request

        bq_table_id: A string representing the destination table
        json_data: An array of JSON objects representing the data to stream
        """
        stream_limit = 10000
        table_ref = self.dataset_ref.table(bq_table_id)
        table = self.bigquery_client.get_table(table_ref)
        if json_data:
            try:
                for i in range(0, len(json_data), stream_limit):
                    json_data_chunk = json_data[i:i + stream_limit]
                    errors = self.bigquery_client.create_rows(
                        table, json_data_chunk)
                    if not errors:
                        self.logger.info('{} rows loaded into {}:{}.'.format(
                            len(json_data_chunk), dataset_id, bq_table_id))
                    else:
                        self.logger.error('There was a error while loading data into {}:{}.'.format(
                            dataset_id, bq_table_id))
                        # pprint(errors)
            except Exception as e:
                self.logger.error(str(e))
        else:
            self.logger.warn('No data to load.')

    def __get_table_schema(self, bq_table_id):
        """ Private method to copy table schema

        bq_table_id: A string representing table id from which the schema should be copied
        """
        try:
            table_ref = self.dataset_ref.table(bq_table_id)
            table = self.bigquery_client.get_table(table_ref)
            return table.schema
        except Exception as e:
            self.logger.error(str(e))

    def create_empty_table_by_copy(self, old_table_id, new_table_id):
        """ Public method to create a new empty table by copying the old none
        It copies the schema and creates an empty table from it in the same dataset

        old_table_id: A string
        new_table_id: A string
        """
        try:
            table_ref = self.dataset_ref.table(new_table_id)
            table = bigquery.Table(table_ref)
            table.schema = self.__get_table_schema(old_table_id)
            table = self.bigquery_client.create_table(table)
            self.logger.info('Created table {} in dataset {}.'.format(
                new_table_id, self.dataset_id))
        except Exception as e:
            self.logger.error(str(e))

    def drop_table(self, bq_table_id):
        """ Public method to drop the table

        bq_table_id: A string representing the table to drop
        """
        table_ref = self.dataset_ref.table(bq_table_id)
        self.bigquery_client.delete_table(table_ref)
        self.logger.info('Table {} in dataset {} dropped.'.format(
            bq_table_id, self.dataset_id))


if __name__ == '__main__':
    account_id = os.getenv('CALLRAIL_ACCOUNT_ID')
    token = os.getenv('CALLRAIL_TOKEN')
    project_id = os.getenv('DAN_PROJECT_ID')
    dataset_id = os.getenv('CALLRAIL_DATASET_ID')
    google_servise_account_info = os.getenv('DAN_GOOGLE_CREDENTIALS')
    callrail_details = json.loads(os.getenv('CALLRAIL_DETAILS'))

    yesterday = date.today() - timedelta(1)

    callRail_client = CallRail(
        account_id, token, project_id, dataset_id, google_servise_account_info)

    for detail in callrail_details:
        data = callRail_client.get_calls_from_api(
            yesterday, yesterday, detail['company_id'])
        callRail_client.stream_data_to_bq(detail['bq_table_id'], data)
