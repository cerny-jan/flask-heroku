from google.cloud import bigquery
from google.oauth2 import service_account
import json


class BQ:

    def __init__(self, logger, google_servise_account_info, google_project_id, bq_dataset_id):
        self.logger = logger
        self.dataset_id = bq_dataset_id
        self.__set_google_credentials(google_servise_account_info)
        self.__set_bigquery_client(google_project_id)
        self.__set_dataset_ref(bq_dataset_id)

    def __set_google_credentials(self, google_service_account_info):
        if '.json' in google_service_account_info:
            google_credentials = service_account.Credentials.from_service_account_file(
                google_service_account_info)
        else:
            google_credentials = service_account.Credentials.from_service_account_info(
                json.loads(google_service_account_info))
        self.google_credentials = google_credentials

    def __set_bigquery_client(self, google_project_id):
        bigquery_client = bigquery.Client(
            credentials=self.google_credentials, project=google_project_id)
        self.bigquery_client = bigquery_client

    def __set_dataset_ref(self, bq_dataset_id):
        dataset_ref = self.bigquery_client.dataset(bq_dataset_id)
        self.dataset_ref = dataset_ref

    def load_data_from_file(self, table_id, source_file_name):
        table_ref = self.dataset_ref.table(table_id)
        try:
            with open(source_file_name, 'rb') as source_file:
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = 'text/csv'
                job_config.max_bad_records = 0
                job_config.ignore_unknown_values = True
                job = self.bigquery_client.load_table_from_file(
                    source_file, table_ref, job_config=job_config)
            job.result()  # Waits for job to complete
            self.logger.info('Loaded {} rows into {}:{}.'.format(
                job.output_rows, self.dataset_id, table_id))
        except Exception as e:
            self.logger.error(str(e))

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
            self.logger.info('Created table {}:{}.'.format(
                self.dataset_id, new_table_id))
        except Exception as e:
            self.logger.error(str(e))

    def drop_table(self, bq_table_id):
        """ Public method to drop the table

        bq_table_id: A string representing the table to drop
        """
        table_ref = self.dataset_ref.table(bq_table_id)
        self.bigquery_client.delete_table(table_ref)
        self.logger.info('Dropped table {}:{}.'.format(
            self.dataset_id, bq_table_id))
