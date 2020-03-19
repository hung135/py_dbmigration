import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import os, logging as lg

logging=lg.getLogger() 
import pprint
from py_dbutils.rdbms import postgres as db_utils
import requests 
import json, yaml
from config_parent import Config
from bs4 import BeautifulSoup





TEST_SCHEMA = 'test'
LOGGING_SCHEMA = 'logging'
PROJECT_NAME= 'test_project'
@unittest.skip("skipping")
class Test_api_fetch(unittest.TestCase,Config):
    
    vw_file='/workspace/tests/sql/logging_tables.sql'
    login_url="""https://www.fpds.gov/common/jsp/login.jsp"""
    user_id_field="USERNAME"
    pwd_field="PASSWORD"
    main_url="""https://catalog.data.gov/dataset/federal-procurement-data-system-fpds-api"""
    catalog_url="""https://project-open-data.cio.gov/v1.1/schema/catalog.json"""
    meta_data_url="""https://catalog.data.gov/harvest/object/dc596fe8-2d14-4406-a698-da37ae088573"""
    api_url="""https://www.fpds.gov/fpdsng_cms/"""
    search_url="""https://www.fpds.gov/ezsearch/search.do?q"""
    search_pg1="https://www.fpds.gov/common/jsp/DocumentSearch.jsp?advancedResultsPage=Y&searchType=advanced&requestFromMainPage=N&pageNumber=1&ContractSelectionType=%20A&ContractSelectionType=A&actionSource=searchCriteriaPage&user=&agency_code=&award_piid=&vendor=GREENZONE+SOLUTIONS%2C+INC.&naics_id=&officeID=&officeName=&idvdAgencyAward_id=&signedFromDate=01/01/2018&signedToDate=12/31/2018&modifiedFromDate=&modifiedToDate=&actionCode=&fundingOfficeID=&fundingAgencyID=&obligatedAmountFrom=&obligatedAmountTo=&MINAMOUNT=&MAXAMOUNT=&modNumber=&STATUS=F&undefined=undefined"
    test_session_data="bbbbb"
    page_1=os.path.join(Config.dirs['working_dir'],'page_1')
    print(test_session_data)
    def test_1fetch_meta_api(self):
        #pull on once so we don't have to keep pulling
        if not os.path.exists(os.path.join(self.dirs['working_dir'],'page_1')):
            os.makedirs( self.dirs['working_dir'],exist_ok=True)
            pay_load={self.user_id_field:os.environ["FPDS_USERID"],self.pwd_field:os.environ["FPDS_PWD"]}
            with requests.Session() as session:
                post_response = session.post(self.login_url,data=pay_load)
                r = session.get(self.search_pg1)  
            #print(self.test_session_data)
                print(post_response)
                self.assertTrue(str(post_response)=='<Response [200]>')
                with open(os.path.join(self.dirs['working_dir'],'page_1'),"w") as f:
                    f.write(r.text)
                    
        else:
            print("Previous Seach was written to file, No Fetch Needed")
             
    def test_2process_session_data(self):
        next_search=None
        with open(self.page_1,"r") as f:
            soup = BeautifulSoup(f, 'html.parser')
            priceprice_box = soup.find('href')
            print(priceprice_box)
            for a in soup.find_all('a', href=True):
                if ('/common/jsp/DocumentSearch.jsp?advancedResultsPage') in a['href']:
                    next_search= a['href']
        print(next_search)
        self.assertTrue(next_search is not None)
         
           

    def test_fetch_api(self):
        print("abc")


if __name__ == '__main__':
    unittest.main()
