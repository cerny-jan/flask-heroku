from google.cloud import logging as google_logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.oauth2 import service_account
from bingads.v11.reporting import *
import logging
import json


def get_logger(logger_name, google_service_account_info=None, google_project_id=None):
    """ Helper method to get logger object, if no google details are provived it logs only to console

    logger_name: A string representing logger name, this will propagete to stackdriver if google is used
    google_servise_account_info (optional): A string of JSON object representing your Google service account private JSON key,
        or path to the Google service account private JSON file
    google_project_id (optional): A string representing Google project ID that should be used
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    if google_service_account_info:
        if '.json' in google_service_account_info:
            google_credentials = service_account.Credentials.from_service_account_file(
                google_service_account_info)
        else:
            google_credentials = service_account.Credentials.from_service_account_info(
                json.loads(google_service_account_info))
        # Instantiates a google logging client
        logging_client = google_logging.Client(
            credentials=google_credentials, project=google_project_id)
        # setup logging to google stackdriver
        google_handler = CloudLoggingHandler(
            logging_client, name=logger_name)
        logger.addHandler(google_handler)
    # setup logging to console
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    return logger


def get_bing_account_ids(bing_client):
    """ Helper method to to get all account ids that belong to a user authenticated via bing_client

    bing_client: A Bing object from clients/bing
    """
    user = bing_client.customer_service.GetUser(None).User
    predicates = {
        'Predicate': [
            {
                'Field': 'UserId',
                'Operator': 'Equals',
                'Value': user.Id,
            },
        ]
    }
    paging = {
        'Index': 0,
        'Size': 10
    }
    accounts = bing_client.customer_service.SearchAccounts(
        PageInfo=paging, Predicates=predicates)
    account_ids = []
    for account in accounts['Account']:
        account_ids.append(account.Id)
    return account_ids


def create_keyword_performance_report_request(bing_client, account_ids, time_peried):
    """ Helper method to create KeywordPerformanceReportRequest

    bing_client: A bing client class
    account_ids: An array of Bing acccount AccountIds
    time_peried: A string representing predefined time peried. Possible values are LastFourWeeks, LastMonth, LastSevenDays,
                LastSixMonths, LastThreeMonths, LastWeek, LastYear, ThisMonth, ThisWeek, ThisYear, Today, Yesterday
    """
    report_request = bing_client.reporting_service.factory.create(
        'KeywordPerformanceReportRequest')
    report_request.Format = 'Csv'
    report_request.ReturnOnlyCompleteData = False
    report_request.Aggregation = 'Daily'
    report_request.Language = 'English'
    report_request.ExcludeColumnHeaders = True
    report_request.ExcludeReportFooter = True
    report_request.ExcludeReportHeader = True

    # The scope of the report. Use this element to limit the report to include data for a combination of accounts, ad groups, and campaigns.
    scope = bing_client.reporting_service.factory.create(
        'AccountThroughAdGroupReportScope')
    scope.AccountIds = {'long': account_ids}
    scope.Campaigns = None
    scope.AdGroups = None
    report_request.Scope = scope

    report_time = bing_client.reporting_service.factory.create('ReportTime')
    report_time.PredefinedTime = time_peried
    report_request.Time = report_time

    # The list of attributes and performance statistics to include in the report.
    report_columns = bing_client.reporting_service.factory.create(
        'ArrayOfKeywordPerformanceReportColumn')
    report_columns.KeywordPerformanceReportColumn.append([
        'TimePeriod',
        'AccountId',
        'AccountName',
        'CampaignId',
        'CampaignName',
        'CampaignStatus',
        'AdGroupId',
        'AdGroupName',
        'AdGroupStatus',
        'Keyword',
        'KeywordLabels',
        'KeywordStatus',
        'BidMatchType',
        'Impressions',
        'Clicks',
        'Spend',
        'AveragePosition',
        'QualityScore'
    ])
    report_request.Columns = report_columns
    report_request.MaxRows = 500000
    bing_client.logger.info('Created KeywordPerformanceReportRequest with IDs: {} for timePeriod: {}.'.format(
        str(account_ids), time_peried))
    return report_request
