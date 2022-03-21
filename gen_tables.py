
import pymysql
import pandas as pd
import numpy as np


df = pd.read_excel('final_output2.xlsx')

terms = df['searchterm'].unique()


db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()

# 前90天排名， 前30天排名， 前14天排名，前七天排名
from datetime import timedelta, date
df_week=pd.DataFrame()


# 周期性【周期性增长，周期性稳定，周期性下降】
# 增长 【增长型需求，潜力型增长】
# 下降 【‘衰退型需求’】
# 无周期性稳定期需求

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


df_week['date'] = pd.to_datetime(df_week['date'])

df_week['logging_scr'] = df_week['searchfrequencyrank'].apply(lambda x: np.log(3500000) - np.log(x))

df_week['date'] = df_week['date'].apply(lambda x: datetime.date(x.year, x.month, x.day))

df_week_sel = df_week[['date', 'searchterm', 'logging_scr', 'searchfrequencyrank', 'clickshare_inall', 'conversionshare_inall']].drop_duplicates(['date', 'searchterm'])
df_week_sel['month'] = df_week_sel['date'].apply(lambda x:  x.month)
df_week_sel['year'] = df_week_sel['date'].apply(lambda x: x.year)


def get_val(df, search_tm, last_dt):
    val1 = df[(df['searchterm']==search_tm) & \
                                                        (df['date']==last_dt)]['searchfrequencyrank'].values
    if len(val1)==0:
        val1 = 0

    return val1


def get_max_date():
    db = pymysql.connect(host='106.13.219.126', port=4000,
                         user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
    cursor = db.cursor()

    sel = """ select max(date) from spider.aba_week WHERE domain='amazon.com' """
    cursor.execute(sel)
    result2 = cursor.fetchall()
    recent_dt = result2[0][0]
    recent_dt = datetime.date(recent_dt.year, recent_dt.month, recent_dt.day )
    return  recent_dt


df_lst = []
for search_tm in df_week_sel.groupby('searchterm')['year'].min()[df_week_sel.groupby('searchterm')['year'].min()>=2021].index: # terms
    try:
        df_week_sel1 = df_week_sel[(df_week_sel['searchterm']==search_tm)]
        dt_max = df_week_sel1['date'].max()
        dt_min = df_week_sel1['date'].min()

        try:
            last_year = int(df_week_sel1[(df_week_sel1['year']==get_max_date().year-1) & \
                                     (df_week_sel1['month']>=get_max_date().month-1) & \
                              (df_week_sel1['month']<=get_max_date().month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_year = '未上榜'
        #datetime.date(dt_max.year-1, dt_max.month, calendar.monthrange(dt_max.year-1, dt_max.month)[1]


        try:
            last_30dt = get_max_date() + datetime.timedelta(days=-1*30)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_30 =  int(df_week_sel1[(df_week_sel1['year']==last_30dt.year) &\
            (df_week_sel1['month']>=last_30dt.month-1)& (df_week_sel1['month']<=last_30dt.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_30 = '未上榜'


        try:
            last_120dt = get_max_date() + datetime.timedelta(days=-1*120)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_120 =  int(df_week_sel1[(df_week_sel1['year']==last_120dt.year) &\
            (df_week_sel1['month']>=last_120dt.month-1)& (df_week_sel1['month']<=last_120dt.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_120 = '未上榜'


        try:

            last_240dt = get_max_date() + datetime.timedelta(days=-1*240)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_240=  int(df_week_sel1[(df_week_sel1['year']==last_240dt.year) &\
            (df_week_sel1['month']>=last_240dt.month-1)& (df_week_sel1['month']<=last_240dt.month+1)].sort_values('date')['searchfrequencyrank'].median())

        except:
            last_240 = '未上榜'



        try:
            recent_val = df_week_sel1[(df_week_sel1['date']==get_max_date()) ]['searchfrequencyrank'].iloc[0]
        except:
            recent_val = '未上榜'

        df_lst.append({'searchterm': search_tm,

                        '第一次上榜日期': df_week_sel1['date'].min().strftime('%Y-%m-%d'),

                        '最近一次上榜日期':dt_max.strftime('%Y-%m-%d'),

                        '最近一次上榜排名': df_week_sel1[df_week_sel1['date']==dt_max]['searchfrequencyrank'].iloc[0],

                      #  '上年最高排名':  df_week_sel1[(df_week_sel1['year']==dt_max.year-1)]['searchfrequencyrank'].min(),

                      #  '上年最低排名':  df_week_sel1[(df_week_sel1['year']==dt_max.year-1)]['searchfrequencyrank'].max(),

                        '本季度排名': last_30,

                        '上季度排名': last_120,

                        '上上季度排名': last_240,

                        '上周排名':  recent_val,

                        '去年同期排名': last_year,

                        'labels': '潜力型增长'})
    except:
        print(search_tm)

            #'year1_diff': last_1year_val,
pd.DataFrame(df_lst).to_excel('final_outputaddson.xlsx')


df_output[df_output['searchterm']]
