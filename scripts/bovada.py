import requests  # For making HTTP requests
import pandas as pd  # For data manipulation and creating DataFrames
import numpy as np  # For numerical operations like timedelta handling
from datetime import datetime  # For date and time operations
import pyarrow as pa  # For handling Apache Arrow and Parquet data
import pyarrow.parquet as pq  # For writing Parquet files


headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

def load_url(url, timeout):
  with requests.get(url) as req:
    return req.json()

def get_bovada_data(url_list):
  out=[]
  
  for url in url_list:
      print(url)
      r=requests.get(url,headers=headers)
      out.append(r.json())
#   with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
#       future_to_url = {executor.submit(load_url, url, 60): url for url in url_list}
#       for future in concurrent.futures.as_completed(future_to_url):
#           url = future_to_url[future]
#           try:
#               data = future.result()
#               out.append(data)
#           except:
#             pass

  df=pd.DataFrame()
  for x in out:
    # try:  
        for i in range(pd.json_normalize(x).index.size):
            for j in range(pd.json_normalize(x[i]['events']).index.size):
                for k in range(pd.json_normalize(x[i]['events'][j]['displayGroups']).index.size):
                    for l in range(pd.json_normalize(x[i]['events'][j]['displayGroups'][k]['markets']).index.size):
                        d=pd.json_normalize(x[i]['events'][j]['displayGroups'][k]['markets'][l]['outcomes'])
                        a=pd.json_normalize(x[i]['path'])
                        d['group'] = a['description'].loc[0]
                        d['title'] = x[i]['events'][j]['description'] + ' - ' + x[i]['events'][j]['displayGroups'][k]['markets'][l]['description']
                        d['url'] = url
                        d['date'] = datetime.now()
                        try:
                            df = pd.concat([df, d], sort=False)
                        except:
                            df=d
    # except:
        pass
  print(df.columns)
  df=df.loc[df['price.american'].notnull()]
  df['price.american']=df['price.american'].replace('EVEN',100)
  df['price.american']=df['price.american'].astype('int')
  df['Implied_Probability'] = (1/pd.to_numeric(df['price.decimal']))
  df['date'] = pd.to_datetime(df['date'])
  df['title']=df['group'] + ' - ' +df['title']
  df = df[['date','title','description','price.american','Implied_Probability']]
  
  return df

def get_historical_data():
    df=pd.read_parquet('https://github.com/aaroncolesmith/bet_model/raw/main/bovada_data.parquet', engine='pyarrow')
    df['date'] = pd.to_datetime(df['date'])
    return df

def remove_records(df):
    df['title_desc'] = df['title'] + ' - ' + df['description']
    df=df.sort_values(['title_desc','date'],ascending=True)

    # Removing records that haven't changed much recently -- need to revamp this
    df['price_change'] = df['price.american'] - df['price.american'].shift(1)
    df['time_change'] = df['date'] - df['date'].shift(1)
    df['time_change'] = df['time_change'] / np.timedelta64(1, 'h')
    # may need to update to this and get rid of numpy
    # df['time_change'] = df['time_change'] / pd.to_timedelta(1, unit='h')
    df.loc[df['title_desc'] != df['title_desc'].shift(1), 'diff_title'] = 1
    df['diff_title']=df['diff_title'].fillna(0)
    df.drop(df.loc[(df.price_change == 0)&(df.diff_title == 0)&(df.time_change < 72)&(df.date < df.date.max() - pd.Timedelta(minutes=10))].index, inplace=True)

    del df['price_change']
    del df['time_change']
    del df['diff_title']
    del df['title_desc']


    d=df.groupby(['title','description']).agg({'Implied_Probability':'std','date':['min','max','size']}).reset_index()
    d.columns=['title','description','std_dev','min_date','max_date','count']
    d=d.groupby('title').agg({'description':'nunique','std_dev':'mean','min_date':'min','max_date':'max','count':'sum'}).reset_index()
    d.columns=['title','total_wagers','avg_std_dev','min_date','max_date','total_count']
    d['first_to_last'] = d['max_date'] - d['min_date']
    d['last_update_min']=(pd.to_numeric(datetime.utcnow().strftime("%s")) - pd.to_numeric(d['max_date'].apply(lambda x: x.strftime('%s'))))/60

    d['last_update_days'] = d['last_update_min'] *0.000694444
    d['avg_std_dev']=d['avg_std_dev'].fillna(0)
    df=df.loc[~df.title.isin(d.loc[(d.total_count < 10) & (d.last_update_days > 20)].title.tolist())].reset_index(drop=True)

    return df

def enhance_data(df):
  df['Rolling_Avg']=df.groupby(['title','description'])['Implied_Probability'].transform(lambda x: x.rolling(3, 1).mean())
  df['Prev_Probability']=df.groupby(['title','description'])['Implied_Probability'].transform(lambda x: x.shift(1))
  df['description']=df.description.str.split(" \(#",expand=True)[0]
  df['description'] = df['description'].str.strip()

  return df

def bovada_scatter(df):
  print('in bovada scatter')
  df['Implied_Probability'] = round(df['Implied_Probability'],4)
  df['Prev_Probability'] = round(df['Prev_Probability'],4)
  df['Pct_Change'] = df['Implied_Probability'] - df['Prev_Probability']
  # df['seconds_ago']=(pd.to_numeric(datetime.datetime.utcnow().strftime("%s")) - pd.to_numeric(df['date'].apply(lambda x: x.strftime('%s'))))
  # df['minutes_ago'] = round(df['seconds_ago']/60,2)
  print('doin some logic')
  d=df.loc[(df.Pct_Change.abs() > .01) & (df.date >= df.date.max() - pd.Timedelta(hours=12)) & (df.Pct_Change.notnull())].sort_values('Pct_Change',ascending=False).reset_index(drop=True)
  print('saving file')
  d[['date','title','description','Pct_Change','Implied_Probability','Prev_Probability']].to_csv('./data/bovada_scatter.csv',index=False)
  print('file saved')

url_list = [
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/olympic-games?marketFilterId=def&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/entertainment?marketFilterId=def&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/politics?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/baseball?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/boxing?marketFilterId=def&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/college-basketball?marketFilterId=rank&preMatchOnly=true&lang=en',
'https://www.bovada.lv/services/sports/event/coupon/events/A/description/golf?marketFilterId=def&preMatchOnly=true&lang=en'
]

df1=get_historical_data()
print(df1.index.size)
df2=get_bovada_data(url_list)
df=pd.concat([df1,df2]).sort_values('date',ascending=True).reset_index(drop=True)
print(df.index.size)
del df1
del df2
df=remove_records(df)
print(df.index.size)
df=enhance_data(df)
print('got the data')

# set_dir()
print('directory set')
bovada_scatter(df)


df=df.sort_values(['title','description','date']).reset_index(drop=True)
df['date'] = pd.to_datetime(df['date'])
df['seconds_ago']=(pd.to_numeric(datetime.utcnow().strftime("%s")) - pd.to_numeric(df['date'].apply(lambda x: x.strftime('%s'))))
df['minutes_ago'] = round(df['seconds_ago']/60,2)
df['Prev_Probability']=df.groupby(['title','description'])['Implied_Probability'].transform(lambda x: x.shift(1))
df['Implied_Probability'] = round(df['Implied_Probability'],4)
df['Prev_Probability'] = round(df['Prev_Probability'],4)


# Convert DataFrame to Apache Arrow Table
table = pa.Table.from_pandas(df[['date','title','description','price.american','Implied_Probability','seconds_ago','minutes_ago','Prev_Probability']])

# Parquet with Brotli compression
pq.write_table(table, '.data/bovada_data.parquet',compression='BROTLI')
