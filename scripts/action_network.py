import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import random
import time
from datetime import datetime, timedelta
import statistics
import numpy as np
from bs4 import BeautifulSoup
import ast
import re
from io import StringIO


def req_to_df(r):
  try:
    games_df=pd.json_normalize(r.json()['games'],
                      )[['id','status','start_time','away_team_id','home_team_id','winning_team_id','league_name','season','attendance',
                      'last_play.home_win_pct','last_play.over_win_pct',
                      'boxscore.total_away_points','boxscore.total_home_points','boxscore.total_away_firsthalf_points','boxscore.total_home_firsthalf_points',
                      'boxscore.total_away_secondhalf_points','boxscore.total_home_secondhalf_points','broadcast.network']]
  except:
    try:
      games_df=pd.json_normalize(r.json()['games'],
                  )[['id','status','start_time','away_team_id','home_team_id','winning_team_id','league_name','season','attendance',
                  'boxscore.total_away_points','boxscore.total_home_points','boxscore.total_away_firsthalf_points','boxscore.total_home_firsthalf_points',
                  'boxscore.total_away_secondhalf_points','boxscore.total_home_secondhalf_points']]
    except:
      games_df=pd.json_normalize(r.json()['games'],
                  )[['id','status','start_time','away_team_id','home_team_id','winning_team_id','league_name','season','attendance',
                  ]]


  odds_df=pd.DataFrame()
  for i in range(pd.json_normalize(r.json()['games']).index.size):
    try:
      odds_df=pd.concat([odds_df,
      pd.json_normalize(r.json()['games'][i],
                    'odds',
                    ['id'],
                    meta_prefix='game_',
                    record_prefix='',
                    errors='ignore'
                    )[[ 'game_id',
                      'ml_away', 'ml_home', 'spread_away', 'spread_home', 'spread_away_line','spread_home_line', 'over', 'under', 'draw', 'total', 
                          'away_total','away_over', 'away_under', 'home_total', 'home_over', 'home_under',
        'ml_home_public', 'ml_away_public', 'spread_home_public',
        'spread_away_public', 'total_under_public', 'total_over_public',
        'ml_home_money', 'ml_away_money', 'spread_home_money',
        'spread_away_money', 'total_over_money', 'total_under_money',
        'num_bets', 'book_id','type','inserted'
                          ]]               
    ]
                      ).reset_index(drop=True)
    except:
      pass

  teams_df=pd.DataFrame()
  for i in range(pd.json_normalize(r.json()['games']).index.size):
    teams_df=pd.concat([teams_df,
                        pd.json_normalize(r.json()['games'][i],
                    'teams',
                    ['id'],
                    meta_prefix='game_',
                    record_prefix='team_'
                    )
                        ]
                      ).reset_index(drop=True)

  df=pd.merge(
  pd.merge(
  pd.merge(games_df,
          odds_df.query('book_id == 15'),
          left_on='id',
          right_on='game_id'),
          teams_df[['team_id','team_full_name']].rename(columns={'team_id':'home_team_id', 'team_full_name':'home_team'})
  ),
  teams_df[['team_id','team_full_name']].rename(columns={'team_id':'away_team_id', 'team_full_name':'away_team'})

  )

  df['date_scraped'] = datetime.now()


  return df,teams_df



headers = {
    'Authority': 'api.actionnetwork',
    'Accept': 'application/json',
    'Origin': 'https://www.actionnetwork.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
}



df_cbb=pd.read_parquet('./data/df_cbb.parquet', engine='pyarrow')
df_soccer=pd.read_parquet('./data/df_soccer.parquet', engine='pyarrow')
df_nba=pd.read_parquet('./data/df_nba.parquet', engine='pyarrow')
df_nfl=pd.read_parquet('./data/df_nfl.parquet', engine='pyarrow')
df_mlb=pd.read_parquet('./data/df_mlb.parquet', engine='pyarrow')

try:
    df_nba_futures = pd.read_parquet('./data/df_nba_futures.parquet', engine='pyarrow')
except:
    pass

teams_df=pd.read_parquet('./data/teams_db.parquet', engine='pyarrow')

start_date = datetime.today().date() - timedelta(days=1)
end_date = datetime.today().date() + timedelta(days=7)

current_date = start_date
fail_list=[]

while current_date <= end_date:
    date_str=current_date.strftime('%Y%m%d')
    print(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
    url=f'https://api.actionnetwork.com/web/v1/scoreboard/ncaab?period=game&bookIds=15,30,76,75,123,69,68,972,71,247,79&division=D1&date={date_str}&tournament=0'
    
    # generate a random sleep time between 1 and 6 seconds
    sleep_time = random.randint(1, 6)

    # sleep for the randomly generated time
    time.sleep(sleep_time)
    
    r=requests.get(url,headers=headers)
    print(r.status_code)

    try:
        if len(r.json()['games']) > 0:
            df_tmp,teams_df_tmp=req_to_df(r)

            df_cbb=pd.concat([df_cbb,df_tmp]).reset_index(drop=True)
            teams_df=pd.concat([teams_df,teams_df_tmp]).reset_index(drop=True)
        else:
            print('no games for date')
    except:
        print(date_str + ' failed')
        fail_list.append(date_str)

start_date = datetime.today().date() - timedelta(days=1)
end_date = datetime.today().date() + timedelta(days=7)


current_date = start_date


fail_list=[]

while current_date <= end_date:
    date_str=current_date.strftime('%Y%m%d')
    print(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
    url=f'https://api.actionnetwork.com/web/v1/scoreboard/soccer?period=game&bookIds=15,30,76,75,123,69,68,972,71,247,79&date={date_str}'
    
    # generate a random sleep time between 1 and 6 seconds
    sleep_time = random.randint(1, 6)

    # sleep for the randomly generated time
    time.sleep(sleep_time)
    
    r=requests.get(url,headers=headers)
    print(r.status_code)

    try:
        if len(r.json()['games']) > 0:
            df_tmp,teams_df_tmp=req_to_df(r)

            df_soccer=pd.concat([df_soccer,df_tmp]).reset_index(drop=True)
            teams_df=pd.concat([teams_df,teams_df_tmp]).reset_index(drop=True)
        else:
            print('no games for date')
    except:
        print(date_str + ' failed')
        fail_list.append(date_str)

start_date = datetime.today().date() - timedelta(days=1)
end_date = datetime.today().date() + timedelta(days=7)


# current_date = start_date.date()
current_date = start_date

fail_list=[]

while current_date <= end_date:
    date_str=current_date.strftime('%Y%m%d')
    print(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
    url=f'https://api.actionnetwork.com/web/v1/scoreboard/nba?period=game&bookIds=15,30,76,75,123,69,68,972,71,247,79&date={date_str}'
    
    # generate a random sleep time between 1 and 6 seconds
    sleep_time = random.randint(1, 6)

    # sleep for the randomly generated time
    time.sleep(sleep_time)
    
    r=requests.get(url,headers=headers)
    print(r.status_code)

    try:
        if len(r.json()['games']) > 0:
            df_tmp,teams_df_tmp=req_to_df(r)

            df_nba=pd.concat([df_nba,df_tmp]).reset_index(drop=True)
            teams_df=pd.concat([teams_df,teams_df_tmp]).reset_index(drop=True)
        else:
            print('no games for date')
    except:
        print(date_str + ' failed')
        fail_list.append(date_str)

start_date = datetime.today().date() - timedelta(days=1)
end_date = datetime.today().date() + timedelta(days=7)

# current_date = start_date.date()
current_date = start_date

fail_list=[]

while current_date <= end_date:
    date_str=current_date.strftime('%Y%m%d')
    print(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
    url=f'https://api.actionnetwork.com/web/v1/scoreboard/nfl?period=game&bookIds=15,30,76,75,123,69,68,972,71,247,79&date={date_str}'
    
    # generate a random sleep time between 1 and 6 seconds
    sleep_time = random.randint(1, 6)

    # sleep for the randomly generated time
    time.sleep(sleep_time)
    
    r=requests.get(url,headers=headers)
    print(r.status_code)

    try:
        if len(r.json()['games']) > 0:
            df_tmp,teams_df_tmp=req_to_df(r)

            df_nfl=pd.concat([df_nfl,df_tmp]).reset_index(drop=True)
            teams_df=pd.concat([teams_df,teams_df_tmp]).reset_index(drop=True)
        else:
            print('no games for date')
    except:
        print(date_str + ' failed')
        fail_list.append(date_str)


start_date = datetime.today().date() - timedelta(days=1)
end_date = datetime.today().date() + timedelta(days=7)

# current_date = start_date.date()
current_date = start_date

fail_list=[]

while current_date <= end_date:
    date_str=current_date.strftime('%Y%m%d')
    print(current_date.strftime('%Y%m%d'))
    current_date += timedelta(days=1)
    url=f'https://api.actionnetwork.com/web/v1/scoreboard/mlb?period=game&bookIds=15,30,76,75,123,69,68,972,71,247,79&date={date_str}'
    
    # generate a random sleep time between 1 and 6 seconds
    sleep_time = random.randint(1, 6)

    # sleep for the randomly generated time
    time.sleep(sleep_time)
    
    r=requests.get(url,headers=headers)
    print(r.status_code)

    try:
        if len(r.json()['games']) > 0:
            df_tmp,teams_df_tmp=req_to_df(r)

            df_mlb=pd.concat([df_mlb,df_tmp]).reset_index(drop=True)
            teams_df=pd.concat([teams_df,teams_df_tmp]).reset_index(drop=True)
        else:
            print('no games for date')
    except:
        print(date_str + ' failed')
        fail_list.append(date_str)

def mode_agg(x):
    try:
        return statistics.mode(x)
    except statistics.StatisticsError:
        return None

def get_bet_data(r1):
  df=pd.DataFrame()

  bet_name = r1.json()['name']

  for j in range(len(r1.json()['books'])):
    book_id = r1.json()['books'][j]['book_id']
    d=pd.json_normalize(r1.json()['books'][j]['odds'])
    d['book_id'] = book_id
    df=pd.concat([df,d])
  df['player_id'] = pd.to_numeric(df['player_id'])

  team_df=pd.json_normalize(r1.json()['teams'])[['id','full_name','display_name','abbr','logo']]
  team_df.columns=['team_id','team_name','team_display_name','team_abbr','team_logo']
  if len(r1.json()['players'])>0:
    player_df = pd.json_normalize(r1.json()['players'])[['id','full_name']]
    player_df.columns=['player_id','player_name']

    df=pd.merge(df,
                player_df,
                how='left'
    )
  df=pd.merge(df,
              team_df,
              how='left'
  )

  df['bet_name'] = bet_name

  return df

def get_prob(a):
    odds = 0
    if a < 0:
        odds = (-a)/(-a + 100)
    else:
        odds = 100/(100+a)

    return odds


## Load existing data
try:
  # df = pd.read_parquet('https://github.com/aaroncolesmith/bet_model/blob/main/df_futures.parquet?raw=true', engine='pyarrow')
  df = pd.read_parquet('./data/df_futures.parquet', engine='pyarrow')
except:
  pass

print(f'Input data size: {df.index.size}')


date_scraped=datetime.now()
d=pd.DataFrame()
for league_id in range(3):
  try:
    url=f'https://api.actionnetwork.com/web/v1/leagues/{league_id}/futures/available'
    r=requests.get(url,headers=headers)
    for i in range(len(r.json()['futures'])):
      bet_type = r.json()['futures'][i]['type']
      url = f'https://api.actionnetwork.com/web/v1/leagues/{league_id}/futures/'+bet_type.replace('#','%23')
      r1=requests.get(url,headers=headers)
      d1 = get_bet_data(r1)
      d1['bet_type'] = bet_type
      d1['date_scraped'] = date_scraped
      d=pd.concat([d,d1])
    print(f'League {league_id} succeeded')
  except:
    print(f'League {league_id} failed:')

d = d.reset_index(drop=True)
d.loc[d.player_name.notnull(), 'bet_outcome'] = d['player_name']
d.loc[d.player_name.isna(), 'bet_outcome'] = d['team_name']
d = d.query('bet_outcome != "0"').reset_index(drop=True)

d_agg=d.groupby(['date_scraped','bet_name','bet_type','bet_outcome',
           'value','option_type_id'],dropna=False).agg(
          player_name=('player_name',mode_agg),
          player_id=('player_id',mode_agg),
          team_name=('team_name',mode_agg),
          team_logo=('team_logo',mode_agg),
          min_money=('money','min'),
          median_money=('money','median'),
          avg_money=('money','mean'),
          max_money=('money','max'),
          books=('book_id', lambda x: ','.join(x.astype('str'))),
          book_count=('book_id','nunique')
).reset_index()

d_agg['implied_probability'] = d_agg['median_money'].apply(get_prob)

df = pd.concat([df,d_agg])


def reduce_data(timestamp, group_by_cols, target_value, dataframe):
    # Sort the dataframe by the timestamp column in ascending order
    sorted_df = dataframe.sort_values(timestamp)

    # Create a mask to identify the initial record for each group
    initial_record_mask = sorted_df.groupby(group_by_cols,dropna=False)[timestamp].transform('first') == sorted_df[timestamp]

    # Create a mask to identify the most recent record for each group
    recent_record_mask = sorted_df.groupby(group_by_cols,dropna=False)[timestamp].transform('last') == sorted_df[timestamp]

    # Create a mask to identify records where there was a change in the target value compared to the previous record
    change_mask = sorted_df.groupby(group_by_cols,dropna=False)[target_value].transform(lambda x: x.ne(x.shift()))

    # Apply the masks to the dataframe and return the filtered results
    filtered_df = sorted_df[initial_record_mask | recent_record_mask | change_mask]

    return filtered_df

def updated_reduce_data(timestamp, group_by_cols, target_value, dataframe):
  sorted_df = df.sort_values(group_by_cols+[timestamp]).reset_index(drop=True)

  sorted_df['price_change'] = sorted_df[target_value] - sorted_df[target_value].shift(1)
  sorted_df['time_change'] = sorted_df[timestamp] - sorted_df[timestamp].shift(1)
  sorted_df['time_change'] = sorted_df['time_change'] / np.timedelta64(1, 'h')
  # sorted_df['time_change'] = sorted_df['time_change'] / pd.to_timedelta(1, unit='h')

  sorted_df.loc[sorted_df.groupby(group_by_cols,dropna=False)[timestamp].transform('last') == sorted_df[timestamp],'max_timestamp_group'] = 1
  sorted_df.max_timestamp_group.fillna(0,inplace=True)
  sorted_df.loc[sorted_df.groupby(group_by_cols,dropna=False)[timestamp].transform('first') == sorted_df[timestamp],'min_timestamp_group'] = 1
  sorted_df.max_timestamp_group.fillna(0,inplace=True)

  filtered_df = sorted_df.loc[(sorted_df.min_timestamp_group == 1) | (sorted_df.max_timestamp_group == 1)| (sorted_df.price_change != 0)].reset_index(drop=True)
  
  for col in ['price_change','time_change','max_timestamp_group','min_timestamp_group']:
    del filtered_df[col]
  
  return filtered_df


# Example usage
timestamp = 'date_scraped'
group_by_cols = ['bet_name', 'bet_type', 'bet_outcome', 'value', 'option_type_id']
target_value = 'median_money'
dataframe = df

print(f'Updated data size: {df.index.size}')

# Call the function to get the reduced dataframe
reduced_df = updated_reduce_data(timestamp, group_by_cols, target_value, dataframe)

print(f'Reduced data size: {reduced_df.index.size}')

table = pa.Table.from_pandas(reduced_df)
pq.write_table(table, './data/df_futures.parquet',compression='BROTLI')


teams_df=pd.merge(teams_df, teams_df.groupby(['team_id'])['game_id'].max(),on=['team_id','game_id']).reset_index(drop=True)

df_cbb=df_cbb.drop_duplicates(subset=df_cbb.columns.to_list()[:-1]).reset_index(drop=True)
df_soccer=df_soccer.drop_duplicates(subset=df_soccer.columns.to_list()[:-1]).reset_index(drop=True)
df_nba=df_nba.drop_duplicates(subset=df_nba.columns.to_list()[:-1]).reset_index(drop=True)
df_nfl=df_nfl.drop_duplicates(subset=df_nfl.columns.to_list()[:-1]).reset_index(drop=True)
df_mlb=df_mlb.drop_duplicates(subset=df_mlb.columns.to_list()[:-1]).reset_index(drop=True)

teams_df=teams_df.drop_duplicates(subset=teams_df.columns.to_list()[:-2]).reset_index(drop=True)

# df_cbb.to_csv('df_cbb.csv',index=False)
# Parquet with Brotli compression
table = pa.Table.from_pandas(df_cbb)
pq.write_table(table, './data/df_cbb.parquet',compression='BROTLI')

# df_soccer.to_csv('df_soccer.csv',index=False)
# Parquet with Brotli compression
table = pa.Table.from_pandas(df_soccer)
pq.write_table(table, './data/df_soccer.parquet',compression='BROTLI')


table = pa.Table.from_pandas(df_nba)
pq.write_table(table, './data/df_nba.parquet',compression='BROTLI')

table = pa.Table.from_pandas(df_nfl)
pq.write_table(table, './data/df_nfl.parquet',compression='BROTLI')

table = pa.Table.from_pandas(df_mlb)
pq.write_table(table, './data/df_mlb.parquet',compression='BROTLI')

# df_mma=df_mma.drop_duplicates(subset=df_soccer.columns.to_list()[:-1]).reset_index(drop=True)
# table = pa.Table.from_pandas(df_mma)
# pq.write_table(table, 'df_mma.parquet',compression='BROTLI')

teams_df.drop_duplicates().to_csv('./data/teams_db.csv',index=False)






### FBREF SECTION

headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

def get_proxy():
    r = requests.get('https://www.us-proxy.org/')
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.read_html(StringIO(str(table)))[0]  # Use StringIO to wrap HTML
    i = random.randint(0, df.index.size - 3)
    proxy_string = "{'http': '" + df.loc[i]['IP Address'] + ':' + df.loc[i]['Port'].astype('str') + "'}"
    proxy = ast.literal_eval(proxy_string)
    return proxy

def extract_date_and_league(url):
    # Regular expression to match the date and league part
    match = re.search(r'(\w+-\d+-\d+)-(.*)', url)
    if match:
        date_str = match.group(1)
        league_str = match.group(2)
        return pd.to_datetime(date_str), league_str
    return None, None


def refresh_fbref_data(df):
  df['start_time_pt'] = pd.to_datetime(df['start_time']).dt.tz_convert('US/Pacific')
  date_list=df.sort_values('start_time_pt',ascending=True)['start_time_pt'].dt.date.astype(str).unique().tolist()
  ## last 5 elements from date_list
  date_list_recent = date_list[-5:]

  df_all = pd.read_parquet('https://github.com/aaroncolesmith/data_load/raw/refs/heads/main/data/fb_ref_data.parquet', engine='pyarrow')

  for date in date_list_recent:
    print(date)
    proxy=get_proxy()
    sleep_time=2.0 + np.random.uniform(1,4) +  np.random.uniform(0,1)
    time.sleep(sleep_time)


    url=f'https://fbref.com/en/matches/{date}'
    r = requests.get(url,headers=headers,proxies=proxy)


    soup = BeautifulSoup(r.content, "html.parser")
    all_urls = []
    for td_tag in soup.find_all('td', {"class":"center"}):
        if 'href' in str(td_tag):
            all_urls.append(
                "https://fbref.com" +str(td_tag).split('href="')[1].split('">')[0]
            )


    dfs = pd.read_html(url, header=0, index_col=0)
    df = pd.DataFrame(dfs[0])
    for i in range(1, len(dfs)):
        df = pd.concat([df,dfs[i]])
    df=df.query('Score.notnull()')
    df = df.loc[df.Home!='Home']
    df.reset_index(drop=False,inplace=True)
    df['url'] = pd.Series(all_urls)
    df['match_selector'] = df['Home']+' '+df['Score']+' '+df['Away']
    df['date_scraped'] = datetime.now()
    df['date'] = date
    df=df.rename(columns={'xG':'home_xg', 'xG.1':'away_xg'})


    df_all = pd.concat([df_all,df])
    print(df_all.index.size)
    time.sleep(4)


  df_all = df_all.drop_duplicates(subset=['date','Home','Away','Venue','Score'], keep='last')
  # df_all.to_csv('/content/drive/MyDrive/Analytics/fbref_match_data.csv',index=False)

  df_all['date_scraped'] = df_all['date_scraped'].astype(str)
  table = pa.Table.from_pandas(df_all)
  pq.write_table(table, './data/fb_ref_data.parquet',compression='BROTLI')

## to do -- update this to only run once or a few times per day

if datetime.now().hour in (2,12,20):
  refresh_fbref_data(df_soccer)



