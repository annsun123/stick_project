import numpy as np
import dateutil, pymysql
import datetime, calendar, dateutil
import pandas as pd

# df_long = pd.DataFrame(df_lst_long)

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

def gen_df(df_month, asin):
    x = df_month[df_month['asin']==asin].sort_values('date')

    df_dt = pd.DataFrame([
          datetime.date((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month,\
                calendar.monthrange((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, \
                                    (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month)[1]) \
                           for _ in range(-1,int(np.ceil(((max_dtDB - x['date'].min()).days)/30))) if \
                           x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= max_dtDB\
                          ]).rename(columns={0:'date'})


    df_dt = df_dt.merge(x, how='left', on='date').fillna(0)#.set_index('date')
    df_dt['month'] = df_dt['date'].apply(lambda x:  x.month)
    df_dt['year'] = df_dt['date'].apply(lambda x: x.year)

    df_dt = df_dt[(df_dt['year']>=2020) & (df_dt['year']<=2021)]

    df_dt['diff'] = df_dt['logging_scr'].diff().fillna(0)
    df_dtD = df_dt.groupby(['year','month'])['diff'].sum().reset_index()
    df_dtD['rank'] = df_dt.groupby(['year','month'])['searchfrequencyrank'].max().values
    df_dtD['logging_scr'] = df_dt.groupby(['year','month'])['logging_scr'].max().values

    return x, df_dt, df_dtD

asin_dic = []

for search_tm in season_asin:
    season = '季节性'

    try:

        x, df_dt, df_dtD = gen_df(df_month, search_tm)
        min_dt = x['date'].min()
        if (max_dtDB - min_dt).days < 365:
            age_define = '上榜未一年'
        else:
            age_define = '上榜超一年'

        if len(df_dt[df_dt['diff']==0])>len(df_dt)*0.6:
            df_diff = df_dtD[df_dtD['diff']>=0.001]
            diff_scr1 = df_diff.groupby('year')['logging_scr'].sum().reset_index().diff().iloc[1]['logging_scr']
            mont_lst = df_diff.groupby('month')['rank'].nunique()[df_diff.groupby('month')['rank'].nunique()>1].index.tolist()
            diff_scr2 = df_diff[df_diff['month'].isin(mont_lst)].groupby('year')['logging_scr'].sum().reset_index().diff().iloc[1]['logging_scr']


        else:

           # df_diff = df_dtD[df_dtD['logging_scr']>=np.percentile(list(df_dtD['logging_scr'].unique()),40)]
         #   diff_scr1 = df_diff.groupby('year')['logging_scr'].sum().reset_index().diff().iloc[1]['logging_scr']
            mont_lst = df_dtD.groupby('month')['rank'].nunique()[df_dtD.groupby('month')['rank'].nunique()>1].index.tolist()
            diff_scr2 = df_dtD[df_dtD['month'].isin(mont_lst)].groupby('year')['logging_scr'].sum().reset_index().diff().iloc[1]['logging_scr']


        if diff_scr2/len(mont_lst)<0.3 and diff_scr2>0:

            label = '相对稳定_微量上升'

        elif diff_scr2/len(mont_lst)>-0.3 and diff_scr2<0:

            label = '相对稳定_微量下降'

        elif diff_scr2/len(mont_lst)>0.3:

            label = '上涨'

        elif diff_scr2/len(mont_lst)<-0.3:

            label = '下降'

        asin_dic.append({'asin':search_tm, '季度性': season, '涨幅':label, '程度': diff_scr2})

    except:

        asin_dic.append({'asin':search_tm, '季度性': season, '涨幅':'', '程度': np.nan})
        pass


for search_tm in nonseason_asin:

    try:
        consecu_val_lst = []
        x, df_dt, df_dtD = gen_df(df_month, search_tm)
        min_dt = x['date'].min()
        max_dt = x['date'].max()
        season = '非季节性'

        if (max_dtDB - min_dt).days < 365:

            age_define = '上榜未一年'
            if (max_dtDB - max_dt).days>200 or len(x['year'])<2:
                asin_dic.append({'asin':search_tm, '季度性': season, '涨幅':'不活跃搜索词', '程度': np.nan})

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

                else:
                    consecu_val_lst.append(0)

            avg_increase_overTime = sum(map(lambda x:x[1] - x[0], zip(consecu_val_lst[:-1], consecu_val_lst[1:])))

            if avg_increase_overTime<0.2 and avg_increase_overTime>0:

                label = '相对稳定_微量上升'

            elif avg_increase_overTime>-0.2 and avg_increase_overTime<0:

                label = '相对稳定_微量下降'

            elif avg_increase_overTime<0.5 and avg_increase_overTime>0.2:

                label = '上涨'

            elif avg_increase_overTime>0.5:

                label = '潜力上涨'

            elif avg_increase_overTime<-0.2:

                label = '下降'

        else:

            age_define = '上榜超一年'
            if x['year'].max()<2021 or len(x['year'])<2 or (max_dtDB - max_dt).days>200 :
                asin_dic.append({'asin':search_tm, '季度性': season, '涨幅':'不活跃搜索词', '程度': np.nan})
            else:
                X = [_+1 for _ in range(len(x))]
                y = x['logging_scr']
                z1 = np.polyfit(X, y, 1)  #一次多项式拟合，相当于线性拟合
                # p1 = np.poly1d(z1)
                avg_increase_overTime = z1[0]

                if avg_increase_overTime<0.005 and avg_increase_overTime>0:

                    label = '相对稳定_微量上升'

                elif avg_increase_overTime>-0.005 and avg_increase_overTime<0:

                    label = '相对稳定_微量下降'

                elif avg_increase_overTime>0.005:

                    label = '上涨'

                elif avg_increase_overTime<-0.005:

                    label = '下降'

                asin_dic.append({'asin':search_tm, '季度性': season, '涨幅':label, '程度': avg_increase_overTime})

    except:
        season = '非季节性'
        asin_dic.append({'asin':search_tm, '季度性': season, '涨幅': '', '程度':np.nan})
        pass


for asin in non_active_asin:
     asin_dic.append({'asin':search_tm, '季度性': '非季节性', '涨幅':'不活跃搜索词', '程度': np.nan})

df_temp1 = pd.DataFrame(asin_dic)

df_temp1['标签']='none'

df_temp1['标签'] = df_temp1[['季度性', '涨幅']].apply(lambda x: x['季度性'] + ' & '+ x['涨幅'], axis=1)

df_temp1.sort_values(['季度性', '涨幅']).to_csv('./files/result_outputCHK/chk_final.csv')

# df_temp1
