import pandas as pd
import pymysql
import numpy as np
import datetime,calendar
import random

search_tm = search_trm_S[3]

#neporal small desk lamp
# small desk lamp ampulla

db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()
df_month=pd.DataFrame()


try:
    select1 = """select max(date) from spider.aba_week WHERE domain='amazon.com'  """

    cursor.execute(select1)
    result2 = cursor.fetchall()
    max_dt = result2[0][0]
    max_dtDB = datetime.date(max_dt.year, max_dt.month, max_dt.day)

except:
    print('error')



df_lst_short = []

dic_lable={}
df_lst_short=[]
for search_tm in search_trm_S:
    try:

        x = df_weekP[(df_weekP['searchterm']==search_tm)].sort_values('date')
        x['date_diff'] = x.sort_values('date')['date'].diff().iloc[1:]
        dt_max = x['date'].max()
        dt_min = x['date'].min()

        try:
            last_year = int(x[(x['year']==max_dtDB.year-1) & \
                                     (x['month']>=max_dtDB.month-1) & \
                              (x['month']<=max_dtDB.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_year = 3600000
            #datetime.date(dt_max.year-1, dt_max.month, calendar.monthrange(dt_max.year-1, dt_max.month)[1]
        import dateutil

        consecu_val_lst = []
        val_lst = []
        for dt in   pd.DataFrame([
            datetime.date((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month,
                    calendar.monthrange((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, \
                                        (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month)[1]) \
                               for _ in range(int(np.ceil(((x['date'].max() - x['date'].min()).days)/30))) if \
                               x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= x['date'].max()\
                              ]).rename(columns={0:'date'}).values:

            val = x[x['date']==dt[0]]['logging_scr']
            if len(val)>0:
                consecu_val_lst.append(val.iloc[0])
                val_lst.append(val.iloc[0])
            else:
                consecu_val_lst.append(0)

        avg_increase_overTime = sum(map(lambda x:x[1] - x[0], zip(consecu_val_lst[:-1], consecu_val_lst[1:])))

        avg_increase = sum(map(lambda x:x[1] - x[0], zip(val_lst[:-1], val_lst[1:])))

        """
            ### unchosen values ###        
            
      
        try:

            last_240dt = max_dtDB() + datetime.timedelta(days=-1*240)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_240=  int(x[(x['year']==last_240dt.year) &\
            (x['month']>=last_240dt.month-1)& (x['month']<=last_240dt.month+1)].sort_values('date')['searchfrequencyrank'].median())

        except:
            last_240 = 3600000
        
        """

        try:
            last_30dt = max_dtDB + datetime.timedelta(days=-1*30)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_30 =  int(x[(x['year']==last_30dt.year) &\
            (x['month']>=last_30dt.month-1)& (x['month']<=last_30dt.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_30 = 3600000



        try:
            last_120dt = max_dtDB + datetime.timedelta(days=-1*120)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_120 =  int(x[(x['year']==last_120dt.year) &\
            (x['month']>=last_120dt.month-1)& (x['month']<=last_120dt.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_120 = 3600000


        try:
            recent_val = x[(x['date']==max_dtDB) ]['searchfrequencyrank'].iloc[0]
        except:
            recent_val = 3600000

        df_lst_short.append({'searchterm': search_tm,

                        '第一次上榜日期': x['date'].min().strftime('%Y-%m-%d'),

                        '最近一次上榜日期':dt_max.strftime('%Y-%m-%d'),

                        '最近一次上榜排名': x[x['date']==dt_max]['searchfrequencyrank'].iloc[0],

                        '距今多久未上榜': (max_dtDB - dt_max).days,

                        '平均上榜间隔': np.mean(x['date_diff'].iloc[1:].apply(lambda x: int(x.days)).values),

                      #  '上年最高排名':  df_week_sel1[(df_week_sel1['year']==dt_max.year-1)]['searchfrequencyrank'].min(),

                      #  '上年最低排名':  df_week_sel1[(df_week_sel1['year']==dt_max.year-1)]['searchfrequencyrank'].max(),

                        '本季度排名': last_30,

                        '上季度排名': last_120,

                        '平均年度变化(over time)':avg_increase_overTime,

                        '平均年度变化': avg_increase,

                        '上周排名':  recent_val,

                        '去年同期排名': last_year,

                        'labels': '近期上榜'})
    except:
        print(search_tm)


