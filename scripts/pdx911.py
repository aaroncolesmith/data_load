import numpy as np
import pandas as pd
import os

import requests
import re

import pyarrow as pa
import pyarrow.parquet as pq


d=pd.read_parquet('./data/portland_crime_data.parquet', engine='pyarrow')
url='https://www.portlandonline.com/scripts/911incidents.cfm'
dtmp=pd.read_xml(url)
dtmp=dtmp.loc[(dtmp.id.notnull())&(dtmp.title.notnull())].reset_index(drop=True)

dtmp=dtmp[['summary','updated','point']]
dtmp.columns=['TEXT','DATE','COORDS']

dtmp[['CRIME','ADDRESS']] = dtmp['TEXT'].str.split('at',n=1, expand=True)
dtmp[['ADDRESS','CRIME_ID']]=dtmp['ADDRESS'].str.split(' \[',n=1,expand=True)
dtmp['ADDRESS']=dtmp['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')

dtmp['DATE'] = pd.to_datetime(dtmp['DATE'])
dtmp['HOUR'] = dtmp['DATE'].dt.floor('h')
dtmp['DAY'] = dtmp['DATE'].dt.floor('d')
dtmp['DATE_CRIME'] = dtmp['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + dtmp['CRIME']

dtmp[['LATITUDE','LONGITUDE']] = dtmp['COORDS'].str.split(' ',n=1, expand=True)

d = pd.concat([d,dtmp])

d=d.groupby(['DATE','TEXT','COORDS']).size().to_frame('cnt').reset_index().sort_values('DATE',ascending=True).reset_index(drop=True)
del d['cnt']

d[['CRIME','ADDRESS']] = d['TEXT'].str.split('at',n=1, expand=True)
d[['ADDRESS','CRIME_ID']]=d['ADDRESS'].str.split(' \[',n=1,expand=True)
d['ADDRESS']=d['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')
d['DATE']=d['DATE'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Pacific'))
d['DATE_CRIME'] = pd.to_datetime(d['DATE'], utc=False).dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']
d[['LATITUDE','LONGITUDE']] = d['COORDS'].str.split(' ',n=1, expand=True)
# d['DATE'] = pd.to_datetime(d['DATE'])
d['HOUR'] = d['DATE'].dt.floor('h',ambiguous=True)
d['DAY'] = d['DATE'].dt.floor('d')
d['DATE_CRIME'] = d['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']


table = pa.Table.from_pandas(d)
pq.write_table(table, './data/portland_crime_data.parquet',compression='BROTLI',use_deprecated_int96_timestamps=True)
