#!/usr/bin/python

from sas7bdat import SAS7BDAT
 
import re
import io

import zipfile
import pandas as pd
import os, logging as log
runtime_pid=os.getpid()
logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')
logging.setLevel(log.DEBUG)
file_dir = "/Users/hung/Downloads/acs2013_5yr/5-Year/"

fqn = os.path.join(file_dir, 'psam_p56.sas7bdat')

rx = re.compile(r'.*un.*.zip')
rx_sas = re.compile(r'.*sas7bdat')
r = []
for path, dnames, fnames in os.walk(file_dir):
    r.extend([os.path.join(path, x) for x in fnames if rx.search(x)])
 

for file in r:
    print(file)
    archive = zipfile.ZipFile(file, 'r')
    for f in archive.namelist():
        if rx_sas.search(f):
            file_size = os.path.getsize(file) >> 20
            if file_size > 100:
                print("file_size", os.path.getsize(file) >> 20, "MB", os.path.getsize(file))
                xfile = io.BytesIO(archive.read(f))
                df = pd.read_sas(xfile, format='sas7bdat', encoding='iso-8859-1', chunksize=1, iterator=True)
                 

                for chunk in df:
                    print(chunk.columns.values, len(chunk.columns.values), "<<<Column Count")
                    break
            #    print(list(df))
            #    break

# imgdata = archive.read('img_01.png')


""" 
for sasfile in r:
    with SAS7BDAT(sasfile) as f:
        for row in f:
            print(sasfile)
            print row
            print("column count",len(row))
            #if i==1:
            break


print(x)
with SAS7BDAT(fqn) as f:
    for row in f:
        i+=1
        print row
        print("column count",len(row))
        if i==1:
            break
"""
