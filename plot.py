import matplotlib.pyplot as plt

search_tm = "wall bike rack indoor"


db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
cursor = db.cursor()

try:

    select1 = """ select * from spider.aba_monthly where domain='amazon.com' and searchterm = "%s" """
    cursor.execute(select1%search_tm)
    result2 = cursor.fetchall()
    desc = cursor.description
    df_month = pd.DataFrame(result2)
    df_month.columns = [x[0] for x in desc]
    # df_month_sample = df_month_sample.append(df_temp)

except:

    print('error')

df_month['date'] = df_month['date'].apply(lambda x: datetime.date(x.year, x.month, x.day))

df_month['logging_scr'] = df_month['searchfrequencyrank'].apply(lambda x: np.log(3500000) - np.log(x))

x = df_month.sort_values('date') #df_month[df_month['searchterm']==search_tm].sort_values('date')

import dateutil
df_dt = pd.DataFrame([
      datetime.date((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month,\
            calendar.monthrange((x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).year, \
                                (x['date'].min()+dateutil.relativedelta.relativedelta(months=_)).month)[1]) \
                       for _ in range(-1,int(np.ceil(((max_dtDB - x['date'].min()).days)/30))) if \
                       x['date'].min()+dateutil.relativedelta.relativedelta(months=_)<= max_dtDB\
                      ]).rename(columns={0:'date'})


df_dt = df_dt.merge(x, how='left', on='date').fillna(0) #.set_index('date')
df_dt['month'] = df_dt['date'].apply(lambda x:  x.month)
df_dt['year'] = df_dt['date'].apply(lambda x: x.year)

df_dt = df_dt[df_dt['year']>=2020]


plt.plot(df_dt['date'], df_dt['logging_scr'])

df_dt[(df_dt['month']>=7)][['month', 'searchfrequencyrank']]

