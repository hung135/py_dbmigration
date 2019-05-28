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

logging = log.getLogger()
logging.setLevel(log.ERROR)

TEST_SCHEMA = 'test'
LOGGING_SCHEMA = 'logging'
PROJECT_NAME= 'test_project'
class Test_api_fetch(unittest.TestCase):
    HOST = os.environ['PGHOST']
    DATABASE = os.environ['PGDATABASE']
    USERID = 'docker'
    DATA_SCHEMA = 'prey'
    DBPASSWORD = 'docker'
    DBPORT = 5432
      

    db = db_utils.DB(host=HOST, userid=USERID, dbname=DATABASE, schema=DATA_SCHEMA,
                                    pwd=DBPASSWORD,  port=DBPORT)
    db.execute("create schema {}".format(LOGGING_SCHEMA))
    db.execute("drop schema {} cascade".format(TEST_SCHEMA))
    db.execute("create schema {}".format(TEST_SCHEMA))
    db.execute("create schema {}".format(DATA_SCHEMA))
    dirs = {
        'sample_data_dir': "/workspace/_sample_data/",
        'sample_working_dir': "/workspace/_sample_working_dir/",
        'sample_zip_data_dir': "/workspace/_sample_zip_data/"}
    data=None
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
    def test_fetch_meta_api(self):
        pay_load={self.user_id_field:os.environ["FPDS_USERID"],self.pwd_field:os.environ["FPDS_PWD"]}
        with requests.Session() as session:
            post = session.post(self.login_url,data=pay_load)
            r = session.get(self.search_pg1)
            print(r.text)

        #test="""https://www.fpds.gov/ezsearch/search.do?q=greenzone+CONTRACTING_AGENCY_NAME%3A%22BUREAU+OF+THE+FISCAL+SERVICE%22+VENDOR_FULL_NAME%3A%22GREENZONE+SOLUTIONS%2C+INC.%22&s=FPDS.GOV&templateName=1.5.1&indexName=awardfull"""
        
        
        
        
        
        #r=requests.post(self.login_url,data=pay_load)

        
        # print( (r.text))
        # print( (r.json()))
        # print(yaml.dump(r.json()))

    def test_fetch_api(self):
        print("abc")


if __name__ == '__main__':
    unittest.main()
