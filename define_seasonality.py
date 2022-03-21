import pandas as pd
import pymysql
import numpy as np
import datetime
import random

import matplotlib.pyplot as plt
# df_grow=pd.read_csv('./files/growth_st.csv')


####### Function #########

df_month_socks = pd.read_csv('./files/df_month_socks.csv')
df_week_socks = pd.read_csv('./files/df_week_socks.csv')


df = pd.read_excel('final_output2.xlsx')

terms = df['searchterm'].unique()


db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()

# 前90天排名， 前30天排名， 前14天排名，前七天排名
from datetime import timedelta, date



# 周期性【周期性增长，周期性稳定，周期性下降】
# 增长 【增长型需求，潜力型增长】
# 下降 【‘衰退型需求’】
# 无周期性稳定期需求

df_month=pd.DataFrame()
for search_trm in terms:

    try:
        select1 = """select * from spider.aba_monthly WHERE domain='amazon.com' and  searchterm= '%s' """

        cursor.execute(select1%(search_trm))
        result2 = cursor.fetchall()
        desc = cursor.description
        df_temp = pd.DataFrame(result2)
        df_temp.columns = [x[0] for x in desc]
        df_month = df_month.append(df_temp)

    except:
        print('error')


df_week=pd.DataFrame()
for search_trm in terms:

    try:
        select1 = """select * from spider.aba_week WHERE domain='amazon.com' and  searchterm= '%s' """

        cursor.execute(select1%(search_trm))
        result2 = cursor.fetchall()
        desc = cursor.description
        df_temp = pd.DataFrame(result2)
        df_temp.columns = [x[0] for x in desc]
        df_week = df_week.append(df_temp)

    except:
        print('error')


def process_table(df):

    df['date'] = pd.to_datetime(df['date'])

    df['logging_scr'] = df['searchfrequencyrank'].apply(lambda x: np.log(3500000) - np.log(x))

    df['date'] = df['date'].apply(lambda x: datetime.date(x.year, x.month, x.day))

    df_sel = df[['date', 'searchterm', 'logging_scr', 'searchfrequencyrank', 'clickshare_inall', 'conversionshare_inall']].drop_duplicates(['date', 'searchterm'])
    df_sel['month'] = df_sel['date'].apply(lambda x:  x.month)
    df_sel['year'] = df_sel['date'].apply(lambda x: x.year)

    return df_sel

df_week_socksP = process_table(df_week_socks)
df_month_socksP = process_table(df_month_socks)
df_monthP = process_table(df_month)
df_weekP = process_table(df_week)

from statsmodels.tsa.seasonal import seasonal_decompose


from sklearn.metrics.pairwise import cosine_similarity

def define_seasonality(df_month):

    season_terms = []
    nonseason_term = []
    error_terms = []

    df_sel = (df_month.groupby('searchterm')['date'].max() - df_month.groupby('searchterm')['date'].min()).reset_index()
    df_sel['days'] = df_sel['date'].apply(lambda x: x.days)

    for search_trm in df_sel[df_sel['days']>=730]['searchterm'].unique():
    #search_trm = '3t socks'  #'ankle socks'

        try:
            x = df_month[df_month['searchterm']==search_trm][['date','logging_scr']].sort_values('date')
            x['date'] = pd.to_datetime(x['date'])
            import dateutil


            df_dt = pd.DataFrame([ x['date'].min()+dateutil.relativedelta.relativedelta(months=_) for _ in range(int(np.ceil(((x['date'].max() - x['date'].min()).days)/30))) if \
                                   x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= x['date'].max()\
                                  ]).rename(columns={0:'date'})

            df_dt = df_dt.merge(x, how='left', on='date').fillna(0)#.set_index('date')
            df_dt['month'] = df_dt['date'].apply(lambda x:  x.month)
            df_dt['year'] = df_dt['date'].apply(lambda x: x.year)

            df_dt = df_dt[(df_dt['year']>=2020) & (df_dt['year']<=2021)]


            df_dt['diff'] = df_dt['logging_scr'].diff().fillna(0)
            df_dtD = df_dt.groupby(['year','month'])['diff'].sum().reset_index()
            df_dtD1 = df_dtD[df_dtD['diff']>=np.percentile(list(df_dtD['diff'].unique()),70)].\
                groupby('year')['month'].unique().reset_index()['month'].apply(lambda x: [1 if _ in list(x) else 0 for _ in range(1,13) ])

            # df_dtD[df_dtD['diff']>=np.percentile(list(df_dtD['diff'].unique()),60)].groupby('year')['month'].unique().reset_index()['month']
            if df_dtD1.shape[0]>=2:
                a= df_dtD1.iloc[1]
                b= df_dtD1.iloc[0]


                user_similarity=cosine_similarity(np.vstack((a,b)))[0][1]
                if user_similarity>0.5:
                    season=True
                    season_terms.append(search_trm)
                    print('season: ', search_trm)
                else:
                    nonseason_term.append(search_trm)
                    print('nonseason: ', search_trm)
            else:
                nonseason_term.append(search_trm)
                print('nonseason: ', search_trm)
        except:
            error_terms.append(error_terms)

    nonseason_term.append(list(df_sel[df_sel['days']<730]['searchterm'].unique()))


    return  season_terms, nonseason_term, error_terms

season_terms, nonseason_term, error_terms = define_seasonality(df_monthP)


nonseason_term = nonseason_term[0]




