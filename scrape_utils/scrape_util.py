import json
import logging

import requests
from bs4 import BeautifulSoup


def getUrl(urlObj, session):
    r = session.post(urlObj.url, data=urlObj.payload)
    print(r.text)
    return r


def soupHtml(r):
    data_soup = BeautifulSoup(r.text, "html.parser")
    return data_soup


def getSoupTables(soup):
    return soup.find_all('table')


def getSoupRows(soup):
    return soup.find_all('tr')


def getSoupLinks(soup):
    return soup.find_all('a')


# Goes out to the supplied URL and downloads a file
# will return error code if error occurs
def getUrlFile(session, url, file_name, download=True):
    import os.path
    error_status = None
    if os.path.isfile(file_name):
        print("File already exists Not Downloading", file_name)
        error_status = "Duplicate"
    else:
        if download:
            print("Downloading:", file_name)
            r = session.get(url)
            try:
                with open(file_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
            except:
                logging.error("Error Writing file")
                error_status = "IOError"
        else:
            print("Downloading Simulated:", file_name)
            error_status = 'Simulate'
    return error_status


def printSoup(soup, find):
    x = 0
    for line in soup.find_all(find):
        x += 1
        print(line)
        print("Line:", x, line.name, line.contents[0])


class Scrape:
    data_json = None
    data_text = None
    data_soup = None
    session = None

    def __init__(self, endpoint, dataformat=None, username=None, password=None):
        r = requests.get(endpoint)
        self.url = endpoint
        if dataformat is None or dataformat == 'json':
            self.data_json = json.loads(r.text)
        if dataformat == 'text':
            self.data_text = r.text
            self.data_json = json.dumps(r.text)
            self.data_soup = BeautifulSoup(r.text, "html.parser")

    def printx(self):

        print(self.data_text)

        self.login()

    def login(self, password=None):
        payload = {'username': 'hung135@hotmail.com', 'password': password}
        self.session = requests.session()

        r = self.session.post(self.url, data=payload)

        payload2 = {'accept': 'Yes', 'action': 'acceptTandC', 'acceptSubmit': 'Continue'}
        r2 = self.session.post("https://freddiemac.embs.com/FLoan/Data/download.php", data=payload2)

        print(r2.text)

    def getJson(self):

        # with open('test.txt','w') as outfile:
        print(self.url)
        for line in self.data_soup.find_all('form'):
            print(line)
            # print(line.get('name'))

            '''json.dump(self.j,outfile)
            print(self.j['ip'])
            '''
