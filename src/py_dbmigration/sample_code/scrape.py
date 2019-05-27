# import csv, pandas,        sqlalchemy, os
import os
import py_dbmigration.scrape_utils
from py_dbutils.rdbms import postgres as  db_utils
import requests
import re
import pandas as pd
import py_dbmigration.db_logging as lg
import datetime as dt
from bs4 import BeautifulSoup
from pprint import pprint

class getUrl:
	url =None
	payload =None
	
	def __init__(self,url,payload=None):
		self.url=url
		self.payload=payload



db = db_utils.DB(dbschema='compliance',dbname='compliance' )
logger=lg.logger.ImportLogger(db)
session = requests.session()
file_list=[]
file_url=[]
file_tup_list=[]

# #login=getUrl("https://freddiemac.embs.com/FLoan/secure/login.php")
# print ("hello")

RAWFILEPATH='/home/dtdata/source_data/freddie_mac/'
username='hung135@hotmail.com'


form_post_login={'username':username,'password':'Ey_@82LH'}
form_post_disclaimer={'accept':'Yes','action':'acceptTandC','acceptSubmit':'Continue'}

login_post=getUrl("https://freddiemac.embs.com/FLoan/secure/auth.php",form_post_login)
accept_disclaimer=getUrl("https://freddiemac.embs.com/FLoan/Data/download.php",form_post_disclaimer)


scrape_utils.getUrl(login_post,session)
raw=scrape_utils.getUrl(accept_disclaimer,session)


soup=scrape_utils.soupHtml(raw)
a=soup.find_all('a')
for x in a:
	#pprint((x.contents[0]))
	name=x.contents[0]
	if(re.match(re.compile('historical'),name)):
		url="https://freddiemac.embs.com/FLoan/Data/"+x['href']
		print(name)
		tup=(name,url)
		file_list.append(name)
		file_url.append(url)
		file_tup_list.append(tup)
		#scrape_utils.getUrlFile(session,url,name,download=False)

		
#pprint (dir(a))
pprint(a)





trg_db = db_utils.Connection(host='wdcdwl01', database='fred_lpd', dbschema='fred_lpd')
 



delta_query = "select distinct file_name from fred_lpd.meta_source_files where file_name like '%.zip'"

trg_df = pd.read_sql(delta_query, trg_db._conn)

src_df =pd.DataFrame({'file_name':file_list})




result=pd.merge(src_df,trg_df,on='file_name',how='left',indicator=True)
# dict = {"src": src_df, "trg": trg_df}
x=result.query("_merge =='left_only'")
# result = pd.concat(dict)
# result = result.drop_duplicates('file_name', keep=False)
# delta_list = result.file_name.tolist()

#print result.drop_duplicates('file_name', keep=False)

new_files=x.file_name.tolist()
for file in new_files:
	for tup_file in file_tup_list:
		if file==tup_file[0]:
			x=None
			x=scrape_utils.getUrlFile(session,url,RAWFILEPATH+file,download=False)
			if x is None:
				logger.insert_LoadStatus(table_name="None",program_unit="WebScrape",
				program_unit_type_code="ZipFile",file_path=RAWFILEPATH+file,username=username)
			else:
				logger.insert_ErrorLog(error_code=x[:5],error_message=url,program_unit="WebScrape",user_name=username)



			#scrape_utils.getUrlFile(session,url,name,download=False)
	
#logger.print_records(100)