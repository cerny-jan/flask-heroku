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


project_id = os.getenv('DAN_PROJECT_ID')
dataset_id = os.getenv('CALLRAIL_DATASET_ID')
table_id = os.getenv('CALLRAIL_TABLE_ID')
# create google credentials from service account info
service_account_info = json.loads(os.getenv('DAN_GOOGLE_CREDENTIALS'))
google_credentials = service_account.Credentials.from_service_account_info(
            service_account_info)

logger = logging.getLogger('lemberglaw')
logger.setLevel(logging.INFO)

# Instantiates a google logging client
logging_client = google_logging.Client(
    credentials=google_credentials, project=project_id)
# setup logging to google stackdriver
google_handler = CloudLoggingHandler(logging_client, name=table_id)
logger.addHandler(google_handler)

# setup logging to console
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)


def get_calls(start_date, end_date, page=1):
    account_id = os.getenv('CALLRAIL_ACCOUNT_ID')
    api_url = 'https://api.callrail.com/v2/a/'
    headers = {
        'authorization': 'Token token={}'.format(os.getenv('CALLRAIL_TOKEN'))
    }
    payload = {
        'start_date': start_date,
        'end_date': end_date,
        'per_page': '250',
        'page': page
    }
    url = api_url + account_id + '/calls.json'
    r = requests.get(url, params=payload, headers=headers)
    response = r.json()
    if response['page'] == response['total_pages']:
        logger.info('Calls from {} to {} pulled from API. {} request{} needed.'.format(
            start_date, end_date, response['total_pages'], 's were' if response['total_pages'] > 1 else ' was'))
    if response['page'] < response['total_pages']:
        result = response['calls'] + get_calls(start_date, end_date, page + 1)
        return result
    else:
        return response['calls']


def stream_data(bigquery_client, dataset_id, table_id, json_data):
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)
    if json_data:
        try:
            errors = bigquery_client.create_rows(table, json_data)
            if not errors:
                logger.info('{} rows loaded into {}:{}'.format(
                    len(json_data), dataset_id, table_id))
            else:
                logger.error('There was a error while loading data into {}:{}'.format(
                    dataset_id, table_id))
                # pprint(errors)
        except Exception as e:
            logger.error(str(e))
    else:
        logger.warn('No data to load')


def get_table_schema(bigquery_client, dataset_id, table_id):
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)
    return table.schema


def create_table(bigquery_client, dataset_id, table_id, schema):
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table.schema = schema
    table = bigquery_client.create_table(table)
    logger.info('Created table {} in dataset {}.'.format(table_id, dataset_id))


if __name__ == '__main__':
    yesterday = date.today() - timedelta(1)
    yesterday_string = yesterday.strftime('%Y-%m-%d')

    calls = get_calls(yesterday_string, yesterday_string)
    
    # Instantiates a BigQuery client
    bigquery_client = bigquery.Client(
        credentials=google_credentials, project=project_id)

    stream_data(bigquery_client, dataset_id, table_id, calls)

    # table_schema = get_table_schema(bigquery_client,dataset_id,table_id)
    # create_table(bigquery_client,dataset_id,'DebugBulldogCalls',table_schema)
