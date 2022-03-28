import pandas as pd
import pymysql
import numpy as np
import datetime, calendar

from sklearn.metrics.pairwise import cosine_similarity

# 前90天排名， 前30天排名， 前14天排名，前七天排名
from datetime import timedelta, date
import random
import matplotlib.pyplot as plt
# df_grow=pd.read_csv('./files/growth_st.csv')


####### Function #########

#df_month_sample = pd.read_csv('./files/df_month_socks.csv')
#df_week_sample = pd.read_csv('./files/df_week_socks.csv')



db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()

# 前90天排名， 前30天排名， 前14天排名，前七天排名
from datetime import timedelta, date



# 周期性【周期性增长，周期性稳定，周期性下降】
# 增长 【增长型需求，潜力型增长】
# 下降 【‘衰退型需求’】
# 无周期性稳定期需求

def process_table(df):

    df['date'] = pd.to_datetime(df['date'])

    df['logging_scr'] = df['searchfrequencyrank'].apply(lambda x: np.log(3500000) - np.log(x))

    df['date'] = df['date'].apply(lambda x: datetime.date(x.year, x.month, x.day))

    df_sel = df[['date', 'searchterm', 'logging_scr', 'searchfrequencyrank', 'clickshare_inall', 'conversionshare_inall']].drop_duplicates(['date', 'searchterm'])
    df_sel['month'] = df_sel['date'].apply(lambda x:  x.month)
    df_sel['year'] = df_sel['date'].apply(lambda x: x.year)

    return df_sel

def gen_maxDB():
    db = pymysql.connect(host='106.13.219.126', port=4000,
                         user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
    cursor = db.cursor()

    try:
        select1 = """select max(date) from spider.aba_week WHERE domain='amazon.com'  """

        cursor.execute(select1)
        result2 = cursor.fetchall()
        max_dt = result2[0][0]
        max_dtDB = datetime.date(max_dt.year, max_dt.month, max_dt.day)
        return max_dtDB

    except:
        print('error')


    # 周期性【周期性增长，周期性稳定，周期性下降】
    # 增长 【增长型需求，潜力型增长】
    # 下降 【‘衰退型需求’】
    # 无周期性稳定期需求


def define_seasonality(df_month_sample):

   # df_week = process_table(df_week_sample)
    df_month = process_table(df_month_sample)

    season_terms = []
    nonseason_term = []
    error_terms = []

    max_dtDB = gen_maxDB()

    df_sel = (max_dtDB - df_month.groupby('searchterm')['date'].min()).reset_index()
    df_sel['days'] = df_sel['date'].apply(lambda x: x.days)
    non_active_terms = list((df_month.groupby('searchterm')['date'].max() - df_month.groupby('searchterm')['date'].min())[(df_month.groupby('searchterm')['date'].max() \
                                                                                                               - df_month.groupby('searchterm')['date'].min())=='0 days'].index)

    df_sel = df_sel[~df_sel['searchterm'].isin(non_active_terms)]
    for search_trm in df_sel[df_sel['days']>=730]['searchterm'].unique():
    # search_trm = '3t socks'  #'ankle socks'

        try:

            x = df_month[df_month['searchterm']==search_trm][['date','logging_scr']].sort_values('date')
           # x['date'] = pd.to_datetime(x['date'])

            import dateutil

            df_dt = pd.DataFrame([
                datetime.date((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month,\
                        calendar.monthrange((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, \
                                            (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month)[1]) \
                                   for _ in range(-1,int(np.ceil(((x['date'].max() - x['date'].min()).days)/30))) if \
                                   x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= x['date'].max()\
                                  ]).rename(columns={0:'date'})


            df_dt = df_dt.merge(x, how='left', on='date').fillna(0) #.set_index('date')
            df_dt['month'] = df_dt['date'].apply(lambda x:  x.month)
            df_dt['year'] = df_dt['date'].apply(lambda x: x.year)

            df_dt['diff'] = df_dt['logging_scr'].diff().fillna(0)
            df_dt = df_dt[(df_dt['year']>=2020) & (df_dt['year']<=2021)]
            df_dtD = df_dt.groupby(['year','month'])['diff'].sum().reset_index()

            if len(df_dt[df_dt['diff']==0])>len(df_dt)*0.6:

                df_dtD1 = df_dtD[df_dtD['diff']>=0.001].\
                groupby('year')['month'].unique().reset_index()['month'].apply(lambda x: [1 if _ in list(x) else 0 for _ in range(1,13) ])

            else:

                df_dtD1 = df_dtD[df_dtD['diff']>=np.percentile(list(df_dtD['diff'].unique()),65)].\
                    groupby(['year'])['month'].unique().reset_index()['month'].apply(lambda x: [1 if _ in list(x) else 0 for _ in range(1,13) ])

            if df_dtD1.shape[0]>=2:
                a= df_dtD1.iloc[1]
                b= df_dtD1.iloc[0]
                user_similarity = cosine_similarity(np.vstack((a,b)))[0][1]
                print(search_trm, user_similarity)
                if user_similarity>0.4:

                    #season=True
                    season_terms.append(search_trm)
                   # print('season: ', search_trm)
                else:
                    nonseason_term.append(search_trm)
            else:
                nonseason_term.append(search_trm)
        except:
            pass
            nonseason_term.append(search_trm)

    nonseason_term.extend(list(df_sel[df_sel['days']<730]['searchterm'].unique()))

    return  df_month, season_terms, nonseason_term, error_terms, non_active_terms


# search_lst = [df_month['searchterm'].unique()[random.randint(1,len(df_month['searchterm'].unique())-1)] for _ in range(500)]
# df_month_sel = df_month[df_month['searchterm'].isin(search_lst)]
df_month , season_terms, nonseason_term, error_terms, non_active_terms= define_seasonality(df_month_sample)

