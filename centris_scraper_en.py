from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from pprint import pprint
from colorama import Fore
import pandas as pd
import requests
import random
import time
import csv
import sys
import os
import re


# - Scraping the data from: https://www.centris.ca/fr/courtiers-immobiliers

# - Needed to be done if the script won't run:
# Add new cookies and headers. In order to be able to send a POST request, values 'x-centris-uck' and 'x-centris-uc'
# need to be passed into headers.


# The site does not block IP or show CAPTCHA


def get_source(url):
    """Makes a GET request to the web page

    Args:
        url (str): url of a particular broker

    Returns:
        returns the requests Response if the status code is 200, otherwise None
    """

    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'sec-ch-ua-mobile': '?0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
        status_code = r.status_code
        if status_code == 200:
            return r
        elif status_code == 404:
            return None
        else:
            print('Status Code: ', status_code)
            return None
    except Exception as e:
        print(Fore.RED + str(e) + Fore.RESET)
        return None


def get_source_post_requests(data):
    """Makes a POST request to the web page

    Args:
        data (int): Number of the broker, needed for post request

    Returns:
        requests Response if the status code is 200, otherwise None - if the status code is 555 for multiple times, the script shuts down
    """

    cookies = {
    }

    headers = {
        'authority': 'www.centris.ca',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'cache-control': 'no-cache',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'content-type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'x-centris-uck': 'da021be4-2019-4ae6-831c-f20319f504a1',   # This part of the headers is necessary to be added so a POST request can be made
        'x-requested-with': 'XMLHttpRequest',
        'x-centris-uc': '0',  # Also needed
        'origin': 'https://www.centris.ca',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.centris.ca/fr/courtier-immobilier~virginie-vendette~re-max-plus-inc./e7709?view=Summary',
        'accept-language': 'en-US,en;q=0.9',
    }

    payload = {'startPosition': str(data)}
    try:
        r = requests.post('https://www.centris.ca/Broker/GetBrokers', headers=headers, json=payload, cookies=cookies, timeout=10)
        status_code = r.status_code
        if status_code == 200:
            return r
        elif status_code == 404:
            return None
        elif status_code == 555:
            print(Fore.RED + f'Status code: {status_code}', Fore.RESET)

            # The script shuts down if status code 555 is shown for seven times in a row
            global count_555_status_code
            count_555_status_code += 1
            if count_555_status_code >= 7:
                print(Fore.LIGHTRED_EX + 'Session Expired, script has been shut down.' + Fore.RESET)
                sys.exit()

            return get_source_post_requests(data)
        else:
            print('Status code: ', status_code)
            return None
    except Exception as e:
        print(Fore.RED + str(e) + Fore.RESET)
        return None


def extract_data(data):
    """Extracts the needed data from particular broker's page

    Args:
        data (int): Number of the broker, needed for post request
    """

    r = get_source_post_requests(data)
    if not r:
        return

    source = r.json()['d']['Result']['Html']
    soup = BeautifulSoup(source, 'lxml')

    # Full Name
    try:
        full_name = soup.select_one('h1[itemprop="name"]').get_text(strip=True)
    except Exception as e:
        print(e)
        print('Ne moze da nadje <Name>')
        return None
    # First Name, Last Name
    if len(full_name.split()) >= 2:
        first_name, last_name = full_name.rsplit(' ', 1)
    else:
        first_name = full_name
        last_name = ''

    # Broker Type
    broker_type_ = soup.select_one('div[itemprop="jobTitle"]')
    broker_type = broker_type_.get_text(strip=True) if broker_type_ else ''

    # Incorporation
    incorporation = ''
    if broker_type_:
        incorporation = broker_type_.find_next_sibling('div')
        incorporation = incorporation.get_text(strip=True) if incorporation else ''

    # Phone1, Phone2
    phone1, phone2 = '', ''
    all_phones = soup.select('div.broker-info-contact-broker a[itemprop="telephone"]')
    if all_phones:
        if len(all_phones) >= 2:
            phone1 = all_phones[0].get_text(strip=True)
            phone2 = all_phones[1].get_text(strip=True)
        else:
            phone1 = all_phones[0].get_text(strip=True)

    # Email 1, Email 2
    # Getting emails
    email1, email2 = '', ''
    email_link = soup.select_one('button.aOpenLeadGrabber')
    if email_link:
        email_link = email_link.get('href').split('&style_url=')[0]

        r_email = get_source(email_link)
        if r_email:
            emails = re.findall(r'([a-zA-Z0-9+._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)', str(r_email.text))
            if emails:
                if len(set(emails)) >= 2:
                    email1 = emails[0]
                    email2 = emails[1]
                else:
                    email1 = emails[0]

    # Profile access
    profile_access = soup.select_one('div.legacy-reset > meta[content]')
    if profile_access:
        profile_access = profile_access.get('content')
        if '?' in profile_access:
            profile_access = profile_access.split('?')[0]
    else:
        profile_access = ''

    # Agency
    agency = soup.select_one('h2[itemprop="legalName"]')
    agency = agency.get_text(strip=True) if agency else ''

    # Agency Address
    agency_address = soup.select_one('a[title="Google Map"]')
    agency_address = agency_address.get_text(strip=True) if agency_address else ''

    # Agency Phone 1, Agency Phone 2
    agency_phone1, agency_phone2 = '', ''
    agency_phone1_ = soup.select_one('div.broker-info-office-info a[itemprop="telephone"]')
    if agency_phone1_:
        agency_phone1 = agency_phone1_.get_text(strip=True)

        agency_phone2_ = agency_phone1_.find_next_sibling('a')
        agency_phone2 = agency_phone2_.get_text(strip=True) if agency_phone2_ else ''

    # Agency website
    agency_website = soup.select_one('div.broker-info-contact-broker a.btn-outline-primary')
    agency_website = agency_website.get('href') if agency_website else ''

    # Covered Territories
    try:
        covered_territories = soup.select_one('div.broker-summary-more-info').find('h3', text=re.compile('Territoire desservi')).find_next_sibling('div').get_text(strip=True)
    except Exception as e:
        covered_territories = ''

    # Save data
    all_data_list = [first_name, last_name, broker_type, incorporation, phone1, phone2, email1, email2, profile_access, agency,
                     agency_address, agency_phone1, agency_phone2, agency_website, covered_territories]
    csv_writer.writerow(all_data_list)

    global result_count_
    result_count_ -= 1
    print(f'Brokers left: {result_count_}. Current: {full_name}')


def save_data():
    """End function which executes other functions and saves the data to csv"""

    global csv_writer, count_555_status_code
    count_555_status_code = 0

    file_name = 'centris_data.csv'
    with open(file_name, 'w', errors='ignore', newline='', encoding='utf-8') as f:
        col_names = ['First Name', 'Last Name', 'Broker Type', 'Incorporation', 'Phone 1', 'Phone 2', 'Email 1', 'Email 2',
                     'Profile access', 'Agency', 'Agency Address', 'Agency Phone 1', 'Agency Phone 2', 'Agency website',
                     'Covered Territories']

        csv_writer = csv.writer(f)
        csv_writer.writerow(col_names)

        # Using 3 threads
        global result_count_
        url = 'https://www.centris.ca/fr/courtiers-immobiliers'
        r = get_source(url)
        if r:
            soup = BeautifulSoup(r.text, 'lxml')
            result_count = soup.select_one('span.resultCount')
            if result_count:
                result_count = int(result_count.get_text(strip=True).replace(u'\xa0', ''))
                print('Total amount of brokers: ', result_count)

                result_count_ = result_count

                ThreadPool(processes=3).map(extract_data, range(result_count))


########################
# Script gets run
save_data()
########################

# Changing from csv to xlsx
df = pd.read_csv('centris_data_.csv')
df.to_excel('centris_data.xlsx', index=False)
os.startfile('centris_data.xlsx')





