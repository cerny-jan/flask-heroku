from bingads.service_client import ServiceClient
from bingads.authorization import *
from bingads.v11.reporting import *
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse as urlparse
import os


class Bing:
    """ https://docs.microsoft.com/en-us/bingads/guides/walkthrough-desktop-application-python

    """
    def __init__(self, logger, client_id, developer_token, client_state, database_url):
        self.logger = logger
        self.__set_authentication(client_id, client_state)
        self.__set_authorization_data(developer_token)
        self.__set_db_connector(database_url)
        self.__set_customer_servise()
        self.__set_campaign_service()
        self.__set_reporting_service()
        self.__set_reporting_service_manager()

    def __set_authentication(self, client_id, client_state):
        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=client_id,
            oauth_tokens=OAuthTokens(
                access_token=None, access_token_expires_in_seconds=1000, refresh_token=None),
        )
        authentication.state = client_state
        self.authentication = authentication

    def __set_authorization_data(self, developer_token):
        authorization_data = AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=developer_token,
            authentication=self.authentication,
        )
        self.authorization_data = authorization_data

    def __set_db_connector(self, database_url):
        db_url = urlparse.urlparse(database_url)
        conn = psycopg2.connect(
            database=db_url.path[1:],
            user=db_url.username,
            password=db_url.password,
            host=db_url.hostname,
            port=db_url.port
        )
        self.conn = conn

    def __set_customer_servise(self):
        customer_service = ServiceClient(
            'CustomerManagementService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.customer_service = customer_service

    def __set_campaign_service(self):
        campaign_service = ServiceClient(
            service='CampaignManagementService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.campaign_service = campaign_service

    def __set_reporting_service(self):
        reporting_service = ServiceClient(
            service='ReportingService',
            authorization_data=self.authorization_data,
            version=11
        )
        self.reporting_service = reporting_service

    def __set_reporting_service_manager(self):
        reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000
        )
        self.reporting_service_manager = reporting_service_manager

    def __save_refresh_token(self, oauth_tokens):
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
        # Register the callback function to automatically save the refresh token anytime it is refreshed.
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
        self.authorization_data.authentication.token_refreshed_callback = self.__save_refresh_token
        print('callback registered')
        self.authorization_data.authentication.request_oauth_tokens_by_response_uri(
            response_uri=response_uri)
