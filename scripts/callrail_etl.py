#!/usr/bin/env python
"""
Runs callrail pipeline.
"""

import argparse
import os
import datetime
from clients.callrail import CallRail
from clients.bq import BQ
from helpers.helpers import get_logger
import pandas as pd
from pandas.tseries.offsets import MonthEnd, MonthBegin
from google.cloud.bigquery.schema import SchemaField


def run_callrail_calls(dataset_id, accounts, start_date, end_date, check_last_record=False):
    callrail_logger = get_logger(
        dataset_id, GOOGLE_SERVICE_ACCOUNT_INFO, GOOGLE_PROJECT_ID)

    callrail_client = CallRail(
        callrail_logger, CALLRAIL_ACCOUNT_ID, CALLRAIL_TOKEN)

    for account in accounts:
        destination_table = '{}_{}'.format(CALLRAIL_TABLE_PREFIX, account)
        bq = BQ(callrail_logger, GOOGLE_SERVICE_ACCOUNT_INFO,
                GOOGLE_PROJECT_ID, dataset_id)
        if check_last_record:
            query = """
                    SELECT
                    date(max(timestamp(created_at))) as result
                    FROM
                          `{project}.{dataset}.{table}`
                    """
            latest_record_date = bq.query_table(
                query, destination_table)
            if latest_record_date:
                latest_record_date = latest_record_date.to_dataframe()
                start_date = latest_record_date.loc[0, 'result']

        data = callrail_client.get_calls_from_api(
            start_date, end_date, account)
        if data:
            schema = [SchemaField(k, 'STRING') for k in data[0].keys()]
            for i, item in enumerate(data):
                data[i] = {k: str(v) for k, v in data[i].items()}
            bq.load_data_from_json(destination_table, data, schema)


def run_callrail_forms(dataset_id, accounts):
    callrail_logger = get_logger(
        dataset_id, GOOGLE_SERVICE_ACCOUNT_INFO, GOOGLE_PROJECT_ID)

    callrail_client = CallRail(
        callrail_logger, CALLRAIL_ACCOUNT_ID, CALLRAIL_TOKEN)
    for account in accounts:
        data = callrail_client.get_forms_from_api(account)
        if data:
            schema = [SchemaField(k, 'STRING') for k in data[0].keys()]
            for i, item in enumerate(data):
                data[i] = {k: str(v) for k, v in data[i].items()}
            bq = BQ(callrail_logger, GOOGLE_SERVICE_ACCOUNT_INFO,
                    GOOGLE_PROJECT_ID, dataset_id)
            destination_table = '{}_{}'.format(
                CALLRAIL_FORM_TABLE_PREFIX, account)
            bq.drop_table(destination_table)
            bq.load_data_from_json(destination_table, data, schema)


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
    parser.add_argument('-f', '--forms',
                        action='store_true', dest='forms',
                        default=False,
                        help='if used, also forms for the acccount will be pulled (all time)')
    parser.add_argument('--company',
                        action='store',
                        dest='company',
                        required=True,
                        default=None,
                        help='specify prefix of the company, this will set what env variables should be loaded')
    parser.add_argument('--accounts',
                        action='store',
                        dest='accounts',
                        nargs='+',
                        default=None,
                        help='specify accounts to load, by default it will load all company accounts under account id')
    parser.add_argument('--start_date',
                        action='store',
                        dest='start_date',
                        nargs='?',
                        help='string in format year-month-day, e.g 2018-01-31')
    parser.add_argument('--end_date',
                        action='store',
                        dest='end_date',
                        nargs='?',
                        help='string in format year-month-day, e.g 2018-01-31')
    args = parser.parse_args()

    # ----------------
    GOOGLE_SERVICE_ACCOUNT_INFO = os.getenv(
        '{company}_GOOGLE_CREDENTIALS'.format(company=args.company.upper()), None)
    GOOGLE_PROJECT_ID = os.getenv(
        '{company}_GOOGLE_PROJECT_ID'.format(company=args.company.upper()), None)

    CALLRAIL_ACCOUNT_ID = os.getenv(
        '{company}_CALLRAIL_ACCOUNT_ID'.format(company=args.company.upper()), None)
    CALLRAIL_TOKEN = os.getenv(
        '{company}_CALLRAIL_TOKEN'.format(company=args.company.upper()), None)

    CALLRAIL_TABLE_PREFIX = 'CALLS'
    CALLRAIL_FORM_TABLE_PREFIX = 'FORMS_{company}_NEW'.format(
        company=args.company.upper())

    CALLRAIL_BQ_DATASET_ID = 'dev' if args.development else os.getenv(
        'CALLRAIL_BQ_DATASET_ID', 'CallrailData')

    if not all([GOOGLE_SERVICE_ACCOUNT_INFO, GOOGLE_PROJECT_ID, CALLRAIL_ACCOUNT_ID, CALLRAIL_TOKEN]):
        raise ValueError(
            'Not all env variables for {} have been created'.format(args.company))

    if args.backfill:
        start_dates = pd.date_range(start=args.start_date, end=pd.to_datetime(
            args.end_date, format="%Y-%m-%d") - MonthBegin(1), freq='MS')
        end_dates = pd.date_range(start=pd.to_datetime(
            args.start_date, format="%Y-%m-%d") + MonthEnd(1), end=args.end_date, freq='M')
        dates = zip(start_dates, end_dates)
        for date in dates:
            run_callrail_calls(CALLRAIL_BQ_DATASET_ID, args.accounts,
                               date[0].strftime('%Y-%m-%d'), date[1].strftime('%Y-%m-%d'))
    elif args.start_date and args.end_date:
        run_callrail_calls(CALLRAIL_BQ_DATASET_ID, args.accounts,
                           args.start_date, args.end_date)
    else:
        yesterday = str(datetime.date.today() - datetime.timedelta(1))
        run_callrail_calls(CALLRAIL_BQ_DATASET_ID, args.accounts,
                           yesterday, yesterday, True)
    if args.forms:
        run_callrail_forms(CALLRAIL_BQ_DATASET_ID, args.accounts)
