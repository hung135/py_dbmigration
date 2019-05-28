import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import logging as log
import os
import pprint
from py_dbutils.rdbms import postgres as db_utils
import requests 
import json, yaml
from config_parent import Config

logging = log.getLogger()
logging.setLevel(log.ERROR)

TEST_SCHEMA = 'test'
LOGGING_SCHEMA = 'logging'
PROJECT_NAME= 'test_project'
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
    
    print(test_session_data)
    def test_1fetch_meta_api(self):
        #pull on once so we don't have to keep pulling
        if not os.path.exists(os.path.join(self.dirs['working_dir'],'page_1')):
            pay_load={self.user_id_field:os.environ["FPDS_USERID"],self.pwd_field:os.environ["FPDS_PWD"]}
            with requests.Session() as session:
                _ = session.post(self.login_url,data=pay_load)
                r = session.get(self.search_pg1)  
            #print(self.test_session_data)
                with open(os.path.join(self.dirs['working_dir'],'page_1')) as f:
                    f.write(r.text)
             
            print("test1")
    def test_2process_session_data(self):
        print(self.test_session_data)
        print("test2")
        #test="""https://www.fpds.gov/ezsearch/search.do?q=greenzone+CONTRACTING_AGENCY_NAME%3A%22BUREAU+OF+THE+FISCAL+SERVICE%22+VENDOR_FULL_NAME%3A%22GREENZONE+SOLUTIONS%2C+INC.%22&s=FPDS.GOV&templateName=1.5.1&indexName=awardfull"""
        
        
        
        
        
        #r=requests.post(self.login_url,data=pay_load)

        
        # print( (r.text))
        # print( (r.json()))
        # print(yaml.dump(r.json()))

    def test_fetch_api(self):
        print("abc")


if __name__ == '__main__':
    unittest.main()
