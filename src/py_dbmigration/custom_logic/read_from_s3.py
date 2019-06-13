#!/usr/bin/python

from sas7bdat import SAS7BDAT
 
import re
import io
from smart_open import smart_open
import zipfile
import pandas as pd
import boto3
import gc

import os, logging as log
runtime_pid=os.getpid()
logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')
logging.setLevel(log.DEBUG)

rx = re.compile(r'.*un.*.zip')
rx_sas = re.compile(r'.*sas7bdat')

s3 = boto3.resource('s3')

bucket = s3.Bucket('sandbox-cdo')
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
#r.extend([os.path.join(path, x) for x in fnames if rx.search(x)])

def genbytes(lines):
    for i in lines:
         yield i

for obj in bucket.objects.all() :
    key = obj.key
    if rx.search(key) and 6 <=(obj.size >>20) <= 7  :


        gc.collect()
        print(key,round(obj.size/(1024000),3),obj.size >>20)
        #print(dir(obj.get()['Body'].read()))
        print("reading...",key)

        archive=None
        with smart_open('s3://sandbox-cdo/{}'.format(key), 'rb') as fin:

            archive = zipfile.ZipFile(fin)

            #body = obj.get()['Body'].read()
            #print(body)
             
            for f in archive.namelist():
                if rx_sas.search(f):

                    print("fileName",f)

                    #xfile = io.BytesIO(archive.read(f))
                    #with smart_open(fin, 'rb') as xfile:
                    with io.BufferedReader(archive.open(f,'r')) as xfile:

                         
                        x = io.BytesIO(xfile.read() )
                        df = pd.read_sas(x, format='sas7bdat', encoding='iso-8859-1', chunksize=1, iterator=True)
                         
                        
                        for chunk in df:
                            #print((chunk.columns.values))
                            print("column_count",len(chunk.columns.values) )
                            break
