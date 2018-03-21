from helpers.helpers import get_logger
from clients.bq import BQ
from clients.callrail import CallRail
from datetime import date, timedelta
import os
import json
import sys


GOOGLE_SERVISE_ACCOUNT_INFO = os.getenv('DAN_GOOGLE_CREDENTIALS')
GOOGLE_PROJECT_ID = os.getenv('DAN_PROJECT_ID')
DATABASE_URL = os.getenv('DATABASE_URL')

CALLRAIL_ACCOUNT_ID = os.getenv('CALLRAIL_ACCOUNT_ID')
CALLRAIL_TOKEN = os.getenv('CALLRAIL_TOKEN')
CALLRAIL_BQ_DATASET_ID = 'CallrailData'
CALLRAIL_DETAILS = json.loads(os.getenv('CALLRAIL_DETAILS'))


if __name__ == '__main__':
    # option to force dev from command line
    if 'dev' in sys.argv[1:]:
        CALLRAIL_BQ_DATASET_ID = 'dev'
        BING_BQ_DATASET_ID = 'dev'

    # CallRail ETL
    callrail_logger = get_logger(
        CALLRAIL_BQ_DATASET_ID, GOOGLE_SERVISE_ACCOUNT_INFO, GOOGLE_PROJECT_ID)
    yesterday = date.today() - timedelta(1)

    callrail_client = CallRail(
        callrail_logger, CALLRAIL_ACCOUNT_ID, CALLRAIL_TOKEN)

    callrail_bq = BQ(callrail_logger, GOOGLE_SERVISE_ACCOUNT_INFO,
                     GOOGLE_PROJECT_ID, CALLRAIL_BQ_DATASET_ID)

    for detail in CALLRAIL_DETAILS:
        data = callrail_client.get_calls_from_api(
            yesterday, yesterday, detail['company_id'])
        callrail_bq.stream_data_to_bq(detail['bq_table_id'], data)
