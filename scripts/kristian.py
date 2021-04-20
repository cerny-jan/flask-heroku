from bs4 import BeautifulSoup
import requests
import logging
import logging.handlers
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io
import os
import re
import json

log_stringIO = io.StringIO()
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)
# file_handler = logging.FileHandler('log.log')
stream_handler = logging.StreamHandler(log_stringIO)
handler = logging.handlers.MemoryHandler(capacity=1024 * 10)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S',)
stream_handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


header = ['Datum', 'Den v tydnu', 'Veprove vypecky',
          'Vaha', 'Cena', 'Pocasi', 'Teplota']
czech_days = ['Pondělí', 'Úterý', 'Středa',
              'Čtvrtek', 'Pátek', 'Sobota', 'Neděle']
recipients = ['xcerj107@gmail.com']
debug = False


def error_handler(recipients, log):
    if not debug and ('error' in log_stringIO.getvalue().lower() or 'warn' in log_stringIO.getvalue().lower()):
        sender = os.environ['DEVEL_EMAIL']
        pswd = os.environ['DEVEL_PSWD']
        subject = 'Kristian script error'
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ','.join(recipients)
        msg['Subject'] = subject
        body = log_stringIO.getvalue()
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, pswd)
        server.sendmail(sender, recipients, msg.as_string())
        logger.info('Email alert sent to {}'.format(','.join(recipients)))
        server.quit()
    log_stringIO.close()


def get_data():
    result = {}
    for item in header:
        result[item] = ''
    now = datetime.now()
    result['Datum'] = now.strftime('%d.%m.%Y')
    result['Den v tydnu'] = czech_days[now.weekday()]
    try:
        link = 'http://www.ukristiana.cz/'
        page = requests.get(link)
        logger.info('Loading: {}'.format(link))
        soup = BeautifulSoup(page.text, 'html.parser')
        menu_date = soup.select(
            '#restaurace-ukristiana .row.no-gutters.meals span')
        menu_date = menu_date[0].string.split(' ') if menu_date else ''
        if menu_date and menu_date[1] == result['Datum']:
            logger.info('Current menu found')
            menu_rows = soup.select('.row.ukristiana-hlavnijidla div')
            for row in menu_rows:
                if 'vepřové výpečky' in row.get_text(strip=True).lower():
                    result['Vaha'] = row.find(
                        'div', class_='block-count').string
                    result['Cena'] = row.find(
                        'div', class_='block-price').string
                    sides = row.get_text(strip=True).replace(
                        result['Cena'], '').replace(result['Vaha'], '').split(',')[1:]
                    result['Vaha'] = int(re.findall(
                        r'\d+', result['Vaha'])[0]) if len(re.findall(r'\d+', result['Vaha'])) > 0 else ''
                    result['Cena'] = int(re.findall(
                        r'\d+', result['Cena'])[0]) if len(re.findall(r'\d+', result['Cena'])) > 0 else ''
                    result['Veprove vypecky'] = ', '.join(
                        list(map(str.strip, sides)))
            if not result['Veprove vypecky']:
                logger.warning('{} - Vypecky not found'.format(link))
                result['Veprove vypecky'] = 'Nemaj výpečky!'
            else:
                logger.info('Vypecky found')
        else:
            logger.warning('{} - Current menu not found'.format(link))
            result['Veprove vypecky'] = 'Menu pro dnešek nenalezeno'
        # weather API
        key = os.environ['WEATHER_API_KEY']
        city_ID = '3067696'
        weather_API = requests.get(
            'http://api.openweathermap.org/data/2.5/weather?id={}&units=metric&APPID={}'.format(city_ID, key))
        if weather_API.status_code == requests.codes.ok:
            if 'main' in weather_API.json():
                logger.info('Weather data loaded')
                result['Teplota'] = weather_API.json()['main']['temp']
                result['Pocasi'] = weather_API.json(
                )['weather'][0]['description']
            else:
                logger.warning('Weather: {}'.format(
                    weather_API.json()['message']))
                result['Pocasi'] = weather_API.json()['message']
        else:
            logger.error('Weather: {} error'.format(weather_API.status_code))
            result['Pocasi'] = 'Unknown'
    except requests.exceptions.Timeout:
        logger.error(
            '{} - TimeoutException: Page is not responding'.format(link))
        result['Veprove vypecky'] = 'TimeoutException'
    except requests.exceptions.ConnectionError:
        logger.error(
            '{} - ConnectionError: network problem (e.g. DNS failure, refused connection, etc)'.format(link))
        result['Veprove vypecky'] = 'ConnectionError'
    except requests.exceptions.HTTPError:
        logger.error('{} - HTTPError:  invalid HTTP response'.format(link))
        result['Veprove vypecky'] = 'HTTPError'
    except requests.exceptions.TooManyRedirects:
        logger.error(
            '{} - TooManyRedirects:  request exceeds the configured number of maximum redirections'.format(link))
        result['Veprove vypecky'] = 'TooManyRedirects'
    except requests.exceptions.RequestException as e:
        logger.error('{} - UnkownError:   {}'.format(link, e))
        result['Veprove vypecky'] = 'UnkownError'
    return result


def clean_vypecky(vypecky):
    clean_sides = ['bramborový knedlík, zelí, cibulka',
                   'bramborový knedlík, špenát, cibulka',
                   'bramborový knedlík, zelí',
                   'bramborový knedlík, špenát',
                   'domácí bramboráčky, zelí',
                   'domácí bramboráčky, špenát',
                   'houskový knedlík, špenát',
                   'houskový knedlík, zelí']
    for i in range(len(clean_sides)):
        if re.search(clean_sides[i].replace(', ', ',').replace(',', '.*'), vypecky):
            return clean_sides[i]
    else:
        return ''


def save_data_to_gspread(filename, data):
    current_data = data
    try:
        credentials = os.environ['GOOGLE_KEY']
        gc = gspread.service_account_from_dict(credentials)
        sheet = gc.open(filename)
        worksheet = sheet.worksheet('raw_data')
        worksheed_header = worksheet.row_values(1)
        new_row = []
        for item in worksheed_header:
            if item in current_data:
                new_row.append(current_data[item])
            else:
                new_row.append('')
        worksheet.append_row(new_row)
        logger.info('Data added')
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(
            'Spreadsheet not found on Google Drive or the file is not shared with client_email from JSON credentials file')
    except FileNotFoundError:
        logger.error('JSON file with credentials not found')


def main():
    logger.info('Script started')
    menu = get_data()
    menu['Veprove vypecky_clean'] = clean_vypecky(menu['Veprove vypecky'])
    save_data_to_gspread('Kristian', menu)
    logger.info('Script finished')

    handler.setTarget(stream_handler)
    handler.flush()
    logger.removeHandler(handler)
    error_handler(recipients, log_stringIO)
    handler.close()


if __name__ == '__main__':
    main()
