from bingads.service_client import ServiceClient
from bingads.authorization import *
from bingads.v11.reporting import *
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse as urlparse


class Bing:
    """ https://docs.microsoft.com/en-us/bingads/guides/walkthrough-desktop-application-python

    """
    def __init__(self, logger, client_id, developer_token, client_state, database_url):
        """  Intialise BigQuery client

        logger: A logger object
        client_id: A string representing the value configured in
            https://docs.microsoft.com/en-us/bingads/guides/authentication-oauth#registerapplication
        developer_token: A string representing the token that you need to use Bing API
            https://docs.microsoft.com/en-us/bingads/guides/get-started#get-developer-token
        client_state: A string representing a non guessable state request parameter to help prevent
            cross site request forgery (CSRF)
        database_url: A string representing database url in the format:
            postgresql://user:secret@localhost
        """
        self.__logger = logger
        self.__set_authentication(client_id, client_state)
        self.__set_authorization_data(developer_token)
        self.__set_db_connector(database_url)
        self.__set_customer_service()
        self.__set_campaign_service()
        self.__set_reporting_service()
        self.__set_reporting_service_manager()

    @property
    def logger(self):
        return self.__logger

    def __set_authentication(self, client_id, client_state):
        """ Private method to set authentication object

        client_id: A string representing the value configured in
            https://docs.microsoft.com/en-us/bingads/guides/authentication-oauth#registerapplication
        client_state: A string representing a non guessable state request parameter to help prevent
            cross site request forgery (CSRF)
        """
        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=client_id,
            oauth_tokens=OAuthTokens(
                access_token=None, access_token_expires_in_seconds=1000, refresh_token=None),
        )
        authentication.state = client_state
        self.authentication = authentication

    def __set_authorization_data(self, developer_token):
        """ Private method to set authorization object

        developer_token: A string representing the token that you need to use Bing API
            https://docs.microsoft.com/en-us/bingads/guides/get-started#get-developer-token
        """
        authorization_data = AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=developer_token,
            authentication=self.authentication,
        )
        self.authorization_data = authorization_data

    def __set_db_connector(self, database_url):
        """ Private method to set database connection

        database_url: A string representing database url in the format:
            postgresql://user:secret@localhost
        """
        db_url = urlparse.urlparse(database_url)
        conn = psycopg2.connect(
            database=db_url.path[1:],
            user=db_url.username,
            password=db_url.password,
            host=db_url.hostname,
            port=db_url.port
        )
        self.conn = conn

    def __set_customer_service(self):
        """ Private method to set customer service client

        """
        customer_service = ServiceClient(
            'CustomerManagementService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.customer_service = customer_service

    def __set_campaign_service(self):
        """ Private method to set campaign service client

        """
        campaign_service = ServiceClient(
            service='CampaignManagementService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.campaign_service = campaign_service

    def __set_reporting_service(self):
        """ Private method to set reporting service client

        """
        reporting_service = ServiceClient(
            service='ReportingService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.reporting_service = reporting_service

    def __set_reporting_service_manager(self):
        """ Private method to set reporting service manager

        """
        reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000
        )
        self.reporting_service_manager = reporting_service_manager

    def __save_refresh_token(self, oauth_tokens):
        """ Private method to save refresh token in database

        oauth_tokens: A object
        """
        sql = """
            UPDATE bing SET (refresh_token,last_update)  =(%s, current_timestamp) WHERE id = 1;
            """
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (oauth_tokens.refresh_token,))
            self.conn.commit()
            cur.close()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(str(e))

    def __get_refresh_token(self):
        """ Private method to retrieve refresh token from database

        """
        sql = """
            SELECT refresh_token FROM bing where id = 1;
             """
        try:
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql)
            bing = cur.fetchone()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(str(e))
        return bing['refresh_token'] if bing else None

    def authenticate(self):
        # Register the callback function to automatically save the refresh token anytime it's refreshed
        self.authorization_data.authentication.token_refreshed_callback = self.__save_refresh_token
        refresh_token = self.__get_refresh_token()
        try:
            # If we have a refresh token let's refresh it
            if refresh_token is not None:
                self.authorization_data.authentication.request_oauth_tokens_by_refresh_token(
                    refresh_token)
            else:
                self.logger.warn('reauth in browser needed')
                self.__request_user_consent()
        except Exception as e:
            self.logger.error(str(e))
            self.__request_user_consent()

    def __request_user_consent(self):
        # this will just return URL to go to, there is no redirect ready after that
        authorization_uri = self.authorization_data.authentication.get_authorization_endpoint()
        self.logger.info('go to this URL: {}'.format(authorization_uri))

    def get_new_refresh_token(self, response_uri):
        # this can be used to get new refresh token, currently this is  not a part of any worflow
        self.authorization_data.authentication.token_refreshed_callback = self.__save_refresh_token
        print('callback registered')
        self.authorization_data.authentication.request_oauth_tokens_by_response_uri(
            response_uri=response_uri)
