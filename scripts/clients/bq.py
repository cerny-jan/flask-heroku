from google.cloud import bigquery
from google.oauth2 import service_account
import json


class BQ:

    def __init__(self, logger, google_servise_account_info, google_project_id, bq_dataset_id):
        """ Intialise BigQuery client

        logger: A logger object
        google_servise_account_info:  A string of JSON object representing your Google service
                account private JSON key, or path to the Google service account private JSON file
        google_project_id: A string representing Google project ID that should be used
        bq_dataset_id: A string representing Google BigQuery dataset ID that should be used
        """

        self.__logger = logger
        self.__project_id = google_project_id
        self.__dataset_id = bq_dataset_id
        self.__set_google_credentials(google_servise_account_info)
        self.__set_bigquery_client(google_project_id)
        self.__set_dataset_ref(bq_dataset_id)

    @property
    def logger(self):
        return self.__logger

    @property
    def project_id(self):
        return self.__project_id

    @property
    def dataset_id(self):
        return self.__dataset_id

    def __set_google_credentials(self, google_service_account_info):
        """ Private method to set Google credentials

        google_service_account_info: A string of JSON object representing your Google service
        account private JSON key, or path to the Google service account private JSON file
        """
        if '.json' in google_service_account_info:
            google_credentials = service_account.Credentials.from_service_account_file(
                google_service_account_info)
        else:
            google_credentials = service_account.Credentials.from_service_account_info(
                json.loads(google_service_account_info))
        self.google_credentials = google_credentials

    def __set_bigquery_client(self, google_project_id):
        """ Private method to set Google BigQuery Client

        google_project_id: A string
        """
        bigquery_client = bigquery.Client(
            credentials=self.google_credentials, project=google_project_id)
        self.bigquery_client = bigquery_client

    def __set_dataset_ref(self, bq_dataset_id):
        """ Private method to set Google Dataset

        bq_dataset_id: A string
        """
        dataset_ref = self.bigquery_client.dataset(bq_dataset_id)
        self.dataset_ref = dataset_ref

    def load_data_from_file(self, bq_table_id, source_file_path):
        """ Public method to load csv file to bigquery

        bq_table_id: A string representing the destination table
        source_file_path: A string representing the full path of the source CSV file
        """
        table_ref = self.dataset_ref.table(bq_table_id)
        try:
            with open(source_file_path, 'rb') as source_file:
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = 'text/csv'
                job_config.autodetect = True
                job_config.max_bad_records = 0
                job_config.ignore_unknown_values = True
                job = self.bigquery_client.load_table_from_file(
                    source_file, table_ref, job_config=job_config)
            job.result()  # Waits for job to complete
            self.logger.info('Loaded {} row{} into {}:{}.'.format(
                job.output_rows, 's' if job.output_rows > 1 else '', self.dataset_id, bq_table_id))
        except Exception as e:
            self.logger.error(str(e))

    def stream_data_to_bq(self, bq_table_id, json_data):
        """ Public method to stream JSON data to bigquery

        Streamed data is available for real-time analysis within a few seconds of the first
        streaming insertion into a table. https://cloud.google.com/bigquery/streaming-data-into-bigquery
        There is a limit of 10k rows per request

        bq_table_id: A string representing the destination table
        json_data: An array of JSON objects representing the data to stream
        """
        stream_limit = 10000
        if json_data:
            try:
                table_ref = self.dataset_ref.table(bq_table_id)
                table = self.bigquery_client.get_table(table_ref)
                for i in range(0, len(json_data), stream_limit):
                    json_data_chunk = json_data[i:i + stream_limit]
                    errors = self.bigquery_client.create_rows(
                        table, json_data_chunk)
                    if not errors:
                        self.logger.info('Loaded {} row{} into {}:{}.'.format(
                            len(json_data_chunk), 's' if len(
                                json_data_chunk) > 1 else '',
                            self.dataset_id, bq_table_id))
                    else:
                        self.logger.error('There was a error while loading data into {}:{}.'.format(
                            self.dataset_id, bq_table_id))
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

    def create_empty_table_by_copy(self, source_table_id, dest_table_id):
        """ Public method to create a new empty table by copying the old none
        It copies the schema and creates an empty table from it in the same dataset

        source_table_id: A string representing a source table
        dest_table_id: A string representing a destination table
        """
        try:
            table_ref = self.dataset_ref.table(dest_table_id)
            table = bigquery.Table(table_ref)
            table.schema = self.__get_table_schema(source_table_id)
            table = self.bigquery_client.create_table(table)
            self.logger.info('Created table {}:{}.'.format(
                self.dataset_id, dest_table_id))
        except Exception as e:
            self.logger.error(str(e))

    def drop_table(self, bq_table_id):
        """ Public method to drop a table

        bq_table_id: A string representing the table to drop
        """
        try:
            table_ref = self.dataset_ref.table(bq_table_id)
            self.bigquery_client.delete_table(table_ref)
            self.logger.info('Dropped table {}:{}.'.format(
                self.dataset_id, bq_table_id))
        except Exception as e:
            self.logger.error(str(e))

    def create_table_by_query(self, query, source_table_id, dest_table_id):
        """ Public method to create a table from query result

        query: A string representing the query that will create new table;
            clausule FROM has to use this format: `{project}.{dataset}.{table}`,
            where all variables are automatically replaced in this method;
            project and dataset from BQ object, table by dest_table_id
        source_table_id: A string representing a source table
        dest_table_id: A string representing a destination table
        """
        try:
            query = query.format(project=self.project_id,
                                 dataset=self.dataset_id, table=source_table_id)
            job_config = bigquery.QueryJobConfig()
            destination_table = self.dataset_ref.table(dest_table_id)
            job_config.destination = destination_table
            job_config.create_disposition = 'CREATE_IF_NEEDED'
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job = self.bigquery_client.query(query, job_config=job_config)
            job.result()
            self.logger.info('Created table {}:{}'.format(
                self.dataset_id, dest_table_id))
        except Exception as e:
            self.logger.error(str(e))
