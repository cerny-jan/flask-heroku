import os
import json
import unittest
import logging
from google.cloud import bigquery

from scripts.clients.callrail import CallRail
from scripts.clients.bq import BQ

CALLRAIL_ACCOUNT_ID = os.getenv('CALLRAIL_ACCOUNT_ID')
CALLRAIL_TOKEN = os.getenv('CALLRAIL_TOKEN')
GOOGLE_SERVISE_ACCOUNT_INFO = os.getenv('DAN_GOOGLE_CREDENTIALS')
GOOGLE_PROJECT_ID = os.getenv('DAN_PROJECT_ID')


class TestCallRail(unittest.TestCase):

    def setUp(self):
        test_logger = logging.getLogger('test')
        self.callrail_client = CallRail(
            test_logger, CALLRAIL_ACCOUNT_ID, CALLRAIL_TOKEN)

    def test_callrail_account_id(self):
        self.assertEqual(
            self.callrail_client.callrail_account_id, CALLRAIL_ACCOUNT_ID)
        with self.assertRaises(AttributeError):
            self.callrail_client.callrail_account_id = 123

    def test_callrail_token(self):
        self.assertEqual(
            self.callrail_client.callrail_token, CALLRAIL_TOKEN)
        with self.assertRaises(AttributeError):
            self.callrail_client.callrail_token = 123

    def test_logger(self):
        self.assertIsInstance(self.callrail_client.logger, logging.Logger)
        with self.assertRaises(AttributeError):
            self.callrail_client.logger = 'test'

    def test_get_calls_from_api(self):
        result = self.callrail_client.get_calls_from_api(
            '2017-12-04', '2017-12-14', '325512356')
        self.assertEqual(len(result), 361, 'Wrong number of records returned')


class TestBQ(unittest.TestCase):
    test_query = """
    SELECT count(*) AS result FROM `{project}.{dataset}.{table}`
    """

    @classmethod
    def setUpClass(self):
        test_logger = logging.getLogger('test')
        self.bq = BQ(test_logger, GOOGLE_SERVISE_ACCOUNT_INFO,
                     GOOGLE_PROJECT_ID, 'test_dataset')
        # create test dataset - normally, the dataset already exists
        dataset_ref = self.bq.bigquery_client.dataset(self.bq.dataset_id)
        self.bq.bigquery_client.create_dataset(bigquery.Dataset(dataset_ref))

    @classmethod
    def tearDownClass(self):
        self.bq.drop_table('shakespeare')
        self.bq.drop_table('shakespeare2')
        self.bq.drop_table('shakespeare3')
        dataset_ref = self.bq.bigquery_client.dataset(self.bq.dataset_id)
        self.bq.bigquery_client.delete_dataset(bigquery.Dataset(dataset_ref))

    def test_a_if_dataset_exists(self):
        self.assertIn('test_dataset', [
                      dataset.dataset_id for dataset in self.bq.bigquery_client.list_datasets()])

    def test_b_load_data_from_file(self):
        source_file_path = os.path.join(os.path.dirname(
            __file__), 'test_files/shakespeare.csv')
        self.bq.load_data_from_file(
            'shakespeare', source_file_path)
        query_job = self.bq.bigquery_client.query(self.test_query.format(
            project=self.bq.project_id, dataset=self.bq.dataset_id, table='shakespeare'))
        self.assertIn(20, [row.result for row in query_job.result()])

    def test_c_stream_data_to_bq(self):
        with self.assertLogs('test') as test_log:
            source_file_path = os.path.join(os.path.dirname(
                __file__), 'test_files/shakespeare.json')
            with open(source_file_path) as json_data:
                self.bq.stream_data_to_bq('shakespeare', json.load(json_data))
        self.assertEqual(test_log.output, [
                         'INFO:test:Loaded 20 rows into test_dataset:shakespeare.'])

    def test_d_create_empty_table_by_copy(self):
        self.bq.create_empty_table_by_copy('shakespeare', 'shakespeare2')
        dataset_ref = self.bq.bigquery_client.dataset(self.bq.dataset_id)
        tables = [table.table_id for table in self.bq.bigquery_client.list_dataset_tables(
            dataset_ref)]
        self.assertIn('shakespeare2', tables)

    def test_e_create_table_by_query(self):
        query = """
            SELECT * FROM  `{project}.{dataset}.{table}`
        """
        self.bq.create_table_by_query(query, 'shakespeare', 'shakespeare3')
        dataset_ref = self.bq.bigquery_client.dataset(self.bq.dataset_id)
        tables = [table.table_id for table in self.bq.bigquery_client.list_dataset_tables(
            dataset_ref)]
        self.assertIn('shakespeare3', tables)


if __name__ == '__main__':
    unittest.main(verbosity=2)
