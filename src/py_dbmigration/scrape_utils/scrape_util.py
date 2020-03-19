import json
 
from lxml import html

import requests
from bs4 import BeautifulSoup
import os, logging as lg

logging=lg.getLogger()


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
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

 

    def __init__(self, endpoint, dataformat='json', login_payload=None,steps=[]):
        r = requests.get(endpoint)
        self.url = endpoint
        self.login_payload=login_payload
        self.steps=steps
        if dataformat is None or dataformat == 'json':
            self.data_json = json.loads(r.text)
        if dataformat == 'text':
            self.data_text = r.text
            self.data_json = json.dumps(r.text)
            self.data_soup = BeautifulSoup(r.text, "html.parser")

        self.steps=steps
        #self.login()
        self.login_selenium()

    def printx(self):

        print(self.data_text)

        self.login()
    def logout(self):
        self.session = requests.session()
    def login_selenium(self):
        from selenium import webdriver


        browser = webdriver.Chrome() #replace with .Firefox(), or with the browser of your choice
        
        browser.get(self.url) #navigate to the page
        username = browser.find_element_by_id("username") #username form field
        password = browser.find_element_by_id("password") #password form field

        username.send_keys(self.payload['username'])
        password.send_keys(self.payload['password'])

        submitButton = browser.find_element_by_id("loginSubmit") 
        button.click() 
        browser.get("http://example.com/page.php") #navigate to page behind login
        innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string

    def login(self):
        self.session = requests.session()
        #payload = {'username': self.username, 'password': self.password}
        r = self.session.post(self.url, data=self.login_payload)
        print("Logged in Response:",r)
        #print(r.text)
    def print_steps(self,steps=None):
        import urllib

        steps_to_print=list(self.steps)
        if steps is not None and len(steps_to_print)==0:
            self.steps=steps
            steps_to_print=steps

        for url in steps_to_print:
            print("Scraping: ",url)
            r=self.session.get(url,headers=self.headers)
            print(r.content)
            #webpage = html.fromstring(r.content)
            #z=webpage.xpath('//a/@href')
            #self.print_steps(z)
    

    def getJson(self):

        # with open('test.txt','w') as outfile:
        print(self.url)
        for line in self.data_soup.find_all('form'):
            print(line)
            # print(line.get('name'))

            '''json.dump(self.j,outfile)
            print(self.j['ip'])
            '''
