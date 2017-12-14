from helpers.helpers import get_logger, get_bing_account_ids, create_keyword_performance_report_request
from clients.bing import Bing
from clients.bq import BQ
from bingads.v11.reporting import *
import os


BING_CLIENT_ID = os.getenv('BING_CLIENT_ID')
BING_DEVELOPER_TOKEN = os.getenv('BING_DEVELOPER_TOKEN')
BING_CLIENT_STATE = os.getenv('BING_CLIENT_STATE')
DATABASE_URL = os.getenv('DATABASE_URL')
GOOGLE_SERVISE_ACCOUNT_INFO = os.getenv('DAN_GOOGLE_CREDENTIALS')
GOOGLE_PROJECT_ID = os.getenv('DAN_PROJECT_ID')
BQ_DATASET_ID = 'dev'
BQ_TABLE_ID = 'Bing'


if __name__ == '__main__':
    logger = get_logger(BQ_TABLE_ID)
    bing = Bing(logger, BING_CLIENT_ID, BING_DEVELOPER_TOKEN, BING_CLIENT_STATE, DATABASE_URL)
    bing.authenticate()

    report_request = create_keyword_performance_report_request(
        bing, get_bing_account_ids(bing), 'LastSixMonths')

    reporting_download_parameters = ReportingDownloadParameters(
        report_request=report_request,
        result_file_directory=os.path.join(os.path.dirname(__file__), 'downloads'),
        result_file_name='download.csv',
        overwrite_result_file=True,
        timeout_in_milliseconds=360000
    )

    result_file_path = bing.reporting_service_manager.download_file(
        reporting_download_parameters)

    if result_file_path:
        logger.info('Downloaded result file.')
        bq = BQ(logger, GOOGLE_SERVISE_ACCOUNT_INFO, GOOGLE_PROJECT_ID, BQ_DATASET_ID)
        bq.load_data_from_file(BQ_TABLE_ID, result_file_path)
        try:
            os.remove(result_file_path)
        except Exception as e:
            logger.error(str(e))
    else:
        logger.warn('No result file.')
