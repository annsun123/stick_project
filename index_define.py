import pandas as pd
import pymysql
import numpy as np
import datetime, calendar
import random

import matplotlib.pyplot as plt
# df_grow=pd.read_csv('./files/growth_st.csv')


####### Function #########

db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()


try:
    select1 = """select max(date) from spider.aba_week WHERE domain='amazon.com'  """

    cursor.execute(select1)
    result2 = cursor.fetchall()
    max_dt = result2[0][0]
    max_dtDB = datetime.date(max_dt.year, max_dt.month, max_dt.day)

except:
    print('error')


df_sel = (df_weekP[df_weekP['searchterm'].isin(nonseason_term)].groupby('searchterm')['date'].max() - \
          df_weekP[df_weekP['searchterm'].isin(nonseason_term)].groupby('searchterm')['date'].min()).reset_index()

df_sel['days'] = df_sel['date'].apply(lambda x: x.days)

search_trm_L = df_sel[df_sel['days']>=365]['searchterm'].unique()
search_trm_S = df_sel[df_sel['days']<=365]['searchterm'].unique()


def gen_dfLongTerm(df_lst, term_lst, season_label,df_monthP,df_weekP):

    for search_tm in term_lst:

        seasonality = season_label

        x = df_monthP[df_monthP['searchterm']==search_tm][['date','logging_scr','searchfrequencyrank']].sort_values('date')

        import dateutil



        df_dt = pd.DataFrame([
            datetime.date((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month,\
                    calendar.monthrange((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, \
                                        (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month)[1]) \
                               for _ in range(int(np.ceil(((x['date'].max() - x['date'].min()).days)/30))) if \
                               x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= x['date'].max()\
                              ]).rename(columns={0:'date'})


        df_dt = df_dt.merge(x, how='left', on='date').fillna(0)#.set_index('date')
        df_dt['month'] = df_dt['date'].apply(lambda x:  x.month)
        df_dt['year'] = df_dt['date'].apply(lambda x: x.year)

        df_dt_part = df_dt[(df_dt['year']>=2020) & (df_dt['year']<=2021)]
        df_dt_part.loc[df_dt_part['searchfrequencyrank']==0,'searchfrequencyrank'] = 3600000

        try:
            all_increaseOYR=[]
            all_increaseAMYR = []

            for mth in range(1,13-1):
                try:

                    all_increaseOYR.append((df_dt_part[(df_dt_part['year']==2021) & (df_dt_part['month']==mth)]['logging_scr'].iloc[0] - \
                                         df_dt_part[(df_dt_part['year']==2020) & (df_dt_part['month']==mth)]['logging_scr'].iloc[0]))
                except:
                    pass


            try:
                for mth in range(1,13-1):
                    all_increaseAMYR.append((df_dt_part[(df_dt_part['year']==2021) & (df_dt_part['month']==mth+1)]['logging_scr'].iloc[0] - \
                                             df_dt_part[(df_dt_part['year']==2021) & (df_dt_part['month']==mth)]['logging_scr'].iloc[0]))
            except:
                pass

            increase_rtOYear = np.mean(all_increaseOYR)

            increase_rtAMYR = np.mean(all_increaseAMYR)


        except:
            increase_rt = 0
            year_increase = 0


        try:
            recent_valscr = df_weekP[(df_weekP['searchterm']==search_tm) & (df_weekP['date']==max_dtDB) ]['logging_scr'].iloc[0]
            recent_valrank = df_weekP[(df_weekP['searchterm']==search_tm) & (df_weekP['date']==max_dtDB) ]['searchfrequencyrank'].iloc[0]
        except:
            recent_valscr = 0
            recent_valrank = 3600000

        try:
            last_yearscr = int(df_weekP[(df_weekP['searchterm']==search_tm)  & (df_weekP['year']==max_dtDB.year-1) & \
                                     (df_weekP['month']>=max_dtDB.month-1) & \
                              (df_weekP['month']<=max_dtDB.month+1)].sort_values('date')['logging_scr'].median())

            last_yearrank = int(df_weekP[(df_weekP['searchterm']==search_tm)  & (df_weekP['year']==max_dtDB.year-1) & \
                                     (df_weekP['month']>=max_dtDB.month-1) & \
                              (df_weekP['month']<=max_dtDB.month+1)].sort_values('date')['searchfrequencyrank'].median())
        except:
            last_yearscr = 0
            last_yearrank = 3600000

        try:
            last_120dt = max_dtDB + datetime.timedelta(days=-1*120)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_120scr =  int(df_dt_part[(df_dt_part['year']==last_120dt.year) &\
            (df_dt_part['month']>=last_120dt.month-1)& (df_dt_part['month']<=last_120dt.month+1)].sort_values('date')['logging_scr'].median())

                # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_120rank =  int(df_dt_part[(df_dt_part['year']==last_120dt.year) &\
            (df_dt_part['month']>=last_120dt.month-1)& (df_dt_part['month']<=last_120dt.month+1)].sort_values('date')['searchfrequencyrank'].median())

        except:
            last_120scr = 0
            last_120rank = 3600000


        try:

            last_240dt = max_dtDB + datetime.timedelta(days=-1*240)
            # last_1quart_val = get_val(df_month_sel, search_tm, one_quarter, dt_max)[0]
            last_240scr =  int(df_dt_part[(df_dt_part['year']==last_240dt.year) &\
            (df_dt_part['month']>=last_240dt.month-1)& (df_dt_part['month']<=last_240dt.month+1)].sort_values('date')['logging_scr'].median())

            last_240rank =  int(df_dt_part[(df_dt_part['year']==last_240dt.year) &\
            (df_dt_part['month']>=last_240dt.month-1)& (df_dt_part['month']<=last_240dt.month+1)].sort_values('date')['searchfrequencyrank'].median())

        except:
            last_240scr = 0
            last_240rank = 3600000





        """if increase_rt>0 and year_increase>0:
            label = 'increase'
        elif increase_rt<0 and year_increase<0:
            label = 'decrease'
        else:
            label = 'blur'
        """


        dt_max = df_weekP[df_weekP['searchterm']==search_tm]['date'].max()
        dt_min = df_weekP[df_weekP['searchterm']==search_tm]['date'].min()

        df_lst.append({'searchterm': search_tm, 'active_dt_range':(dt_max - dt_min).days,

                    'latest_active': (max_dtDB - dt_max).days,

                    '环比增长趋势' : increase_rtOYear,

                    'Last Year 年度增长' : increase_rtAMYR,

                    '近期分值': recent_valscr,
                    '近期分值排名': recent_valrank,

                    '去年同期分值': last_yearscr,
                    '去年同期排名': last_yearrank,

                    '过去240scr': last_240scr,
                    '过去240rank': last_240rank,

                    #'year1_diff': last_1year_val,
                   # 'labels': label,

                    'seasonality': seasonality})

    return  df_lst


# season_terms, nonseason_term, error_terms
# 每个月和之前比是否有上升或是下降
df_lst_long=[]

season_terms = list(df_monthP[df_monthP['searchterm'].isin(season_terms)].groupby('searchterm')['year'].min()[df_monthP[df_monthP['searchterm'].isin(season_terms)].groupby('searchterm')['year'].min()<2021].index)
nonseason_terms = list(df_monthP[df_monthP['searchterm'].isin(nonseason_term)].groupby('searchterm')['year'].min()[df_monthP[df_monthP['searchterm'].isin(nonseason_term)].groupby('searchterm')['year'].min()<=2021].index)

df_lst_long = gen_dfLongTerm(df_lst_long, season_terms, 'season',df_monthP,df_weekP)
df_lst_long = gen_dfLongTerm(df_lst_long, search_trm_L, 'nonseason',df_monthP,df_weekP)


