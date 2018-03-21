#!/usr/bin/env python
"""
Runs bing pipeline.
"""

import argparse
import os
import datetime
from helpers.helpers import get_logger, get_bing_account_ids, create_keyword_performance_report_request
from clients.bq import BQ
from clients.bing import Bing
from bingads.v11.reporting import ReportingDownloadParameters
import pandas as pd
from pandas.tseries.offsets import MonthEnd, MonthBegin


GOOGLE_SERVISE_ACCOUNT_INFO = os.getenv('DAN_GOOGLE_CREDENTIALS')
GOOGLE_PROJECT_ID = os.getenv('DAN_PROJECT_ID')
DATABASE_URL = os.getenv('DATABASE_URL')

BING_CLIENT_ID = os.getenv('BING_CLIENT_ID')
BING_DEVELOPER_TOKEN = os.getenv('BING_DEVELOPER_TOKEN')
BING_CLIENT_STATE = os.getenv('BING_CLIENT_STATE')
BING_BQ_TABLE_ID = 'Bing'


def run_bing(dataset_id, accounts, start_date, end_date):

    bing_logger = get_logger(
        dataset_id, GOOGLE_SERVISE_ACCOUNT_INFO, GOOGLE_PROJECT_ID)

    bing = Bing(bing_logger, BING_CLIENT_ID, BING_DEVELOPER_TOKEN,
                BING_CLIENT_STATE, DATABASE_URL)

    bing.authenticate()

    if not accounts:
        accounts = get_bing_account_ids(bing)

    report_request = create_keyword_performance_report_request(
        bing, accounts, start_date, end_date)

    reporting_download_parameters = ReportingDownloadParameters(
        report_request=report_request,
        result_file_directory=os.path.join(
            os.path.dirname(__file__), 'downloads'),
        result_file_name='download.csv',
        overwrite_result_file=True,
        timeout_in_milliseconds=360000
    )

    try:
        result_file_path = bing.reporting_service_manager.download_file(
            reporting_download_parameters)
        if result_file_path:
            bing_logger.info('Downloaded result file.')
            bq = BQ(bing_logger, GOOGLE_SERVISE_ACCOUNT_INFO,
                    GOOGLE_PROJECT_ID, dataset_id)
            bq.load_data_from_file(BING_BQ_TABLE_ID, result_file_path, 1)
            try:
                os.remove(result_file_path)
                bing_logger.info('Removed result file.')
            except Exception as e:
                bing_logger.error(str(e))
        else:
            bing_logger.warn('No result file.')
    except Exception as e:
        bing_logger.error(str(e))


def create_agg_campaign_table(dataset_id):
    bing_logger = get_logger(
        dataset_id, GOOGLE_SERVISE_ACCOUNT_INFO, GOOGLE_PROJECT_ID)
    bq = BQ(bing_logger, GOOGLE_SERVISE_ACCOUNT_INFO,
            GOOGLE_PROJECT_ID, dataset_id)
    query = """
            SELECT
                    campaign_id,
                    campaign_name,
                    SUM(CAST(impressions AS int64)) AS impressions
            FROM
                    `{project}.{dataset}.{table}`
            GROUP BY
                    1,
                    2
            ORDER BY
                    3 DESC
            """
    bq.create_table_by_query(
        query, BING_BQ_TABLE_ID, 'BingCampaigns')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-d', '--dev',
                        action='store_true', dest='development',
                        default=False,
                        help='forces dev dataset in BigQuery')
    parser.add_argument('-b', '--backfill',
                        action='store_true', dest='backfill',
                        default=False,
                        help='runs batch job by month between start and end date (start date needs to be the first day of the month and end date the last)')
    parser.add_argument('--accounts',
                        action='store',
                        dest='accounts',
                        nargs='+',
                        default=None,
                        help='specify accounts to load, by default it will load all accounts under client id')
    parser.add_argument('--start_date',
                        action='store',
                        dest='start_date',
                        nargs='?',
                        default=str(datetime.date.today() -
                                    datetime.timedelta(1)),
                        help='string in format year-month-day, e.g 2018-01-31')
    parser.add_argument('--end_date',
                        action='store',
                        dest='end_date',
                        nargs='?',
                        default=str(datetime.date.today() -
                                    datetime.timedelta(1)),
                        help='string in format year-month-day, e.g 2018-01-31')
    args = parser.parse_args()

    # ----------------

    BING_BQ_DATASET_ID = 'dev' if args.development else os.getenv(
        'BING_BQ_DATASET_ID', 'BingData')

    if args.backfill:
        start_dates = pd.date_range(start=args.start_date, end=pd.to_datetime(
            args.end_date, format="%Y-%m-%d") - MonthBegin(1), freq='MS')
        end_dates = pd.date_range(start=pd.to_datetime(
            args.start_date, format="%Y-%m-%d") + MonthEnd(1), end=args.end_date, freq='M')
        dates = zip(start_dates, end_dates)
        for date in dates:
            run_bing(BING_BQ_DATASET_ID, args.accounts,
                     date[0].strftime('%Y-%m-%d'), date[1].strftime('%Y-%m-%d'))
        create_agg_campaign_table(BING_BQ_DATASET_ID)

    else:
        run_bing(BING_BQ_DATASET_ID, args.accounts,
                 args.start_date, args.end_date)
        create_agg_campaign_table(BING_BQ_DATASET_ID)
