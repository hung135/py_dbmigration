#!/usr/bin/python

from sas7bdat import SAS7BDAT
import os
import re
import io
from smart_open import smart_open
import zipfile
import pandas as pd
import boto3
import gc

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
    if rx.search(key) and 400<=(obj.size >>20) <= 3000:


        gc.collect()
        print(key,round(obj.size/(1024000),3),obj.size >>20)
        #print(dir(obj.get()['Body'].read()))
        print("reading...",key)

        archive=None
        with smart_open('s3://sandbox-cdo/{}'.format(key), 'rb') as fin:

            archive = zipfile.ZipFile(fin)


        #archive = zipfile.ZipFile(io.BytesIO(genbytes(x)))




            #body = obj.get()['Body'].read()
            #print(body)
            print("xxx")
            for f in archive.namelist():
                if rx_sas.search(f):

                    print(f)

                    xfile = io.BytesIO(archive.read(f))
                    print("yyy")
                    df = pd.read_sas(xfile, format='sas7bdat', encoding='iso-8859-1', chunksize=1, iterator=True)
                    # print(list(df),"xxxxxx")
                    print("zzz")
                    for chunk in df:
                        #print((chunk.columns.values))
                        print("column_count",len(chunk.columns.values) )
                        break
