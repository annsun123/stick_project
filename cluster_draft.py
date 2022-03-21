



Xs = df_month[['logging_scr', 'conversionshare_inall','clickshare_inall']]

from sklearn.mixture import GaussianMixture
gmm = GaussianMixture(n_components=8).fit(Xs)
labels = gmm.predict(Xs)
print('GMM预测值',labels)
Xs['labels'] = labels
Counter(labels)
#

x1_axis = data_TSNE[:,0]
x2_axis = data_TSNE[:,1]
plt.scatter(x1_axis, x2_axis, c=labels)
plt.title("GMM-EM聚类")
plt.show()

Xs['date'] =  df_month['date'].tolist()
Xs['searchterm'] = df_month['searchterm'].tolist()



# 相关联词数产生的asin上涨幅度 【过去30天，过去一个季度，同期上年 涨幅的总和变化】 ---来表示竞争
# 30 日前 (从week里面取)，一季度前，三季度前 【这两个从月里面取】，一年前


for search_tm in [terms_lst[random.randint(0,len(terms_lst)-1)] for _ in range(100)]:
    ind = ind+1
    print(ind)
    dt_max = df_week_sel[(df_week_sel['searchterm']==search_tm)]['date'].max()
    dt_min = df_week_sel[(df_week_sel['searchterm']==search_tm)]['date'].min()
    mth_max = df_month_sel[(df_month_sel['searchterm']==search_tm)]['date'].max()
    mth_min = df_month_sel[(df_month_sel['searchterm']==search_tm)]['date'].min()

    last_30 = dt_max + datetime.timedelta(days=-1*30)
    one_quarter = calculate_dt(mth_max, 3)
    three_quarter = calculate_dt(mth_max, 9)

    try:

        last_30_val = ((df_week_sel[(df_week_sel['searchterm']==search_tm) & \
                                                        (df_week_sel['date']==last_30)]['logging_scr'].values - \
             df_week_sel[(df_week_sel['searchterm']==search_tm) & (df_week_sel['date']==dt_max)]['logging_scr'].values)[0])

    except:
        last_30_val = np.nan

    try:
        last_1quart_val = (df_month_sel[(df_month_sel['searchterm']==search_tm) & \
                                      (df_month_sel['date']==mth_max)]['logging_scr'].values - \
                    df_month_sel[(df_month_sel['searchterm']==search_tm) & (df_month_sel['date']==one_quarter)]['logging_scr'].values)[0]
    except:
        last_1quart_val = np.nan


    try:
        last_3quart_val = (df_month_sel[(df_month_sel['searchterm']==search_tm) & (df_month_sel['date']==mth_max)]['logging_scr'].values - \
                 df_month_sel[(df_month_sel['searchterm']==search_tm) & (df_month_sel['date']==three_quarter)]['logging_scr'].values)[0]

    except:
        last_3quart_val = np.nan



    try:
        last_1year_val = (df_month_sel[(df_month_sel['searchterm']==search_tm) & \
                                   (df_month_sel['date']==mth_max)]['logging_scr'].values - \
            df_month_sel[(df_month_sel['searchterm']==search_tm) & \
                         (df_month_sel['date']==datetime.date(mth_max.year-1, mth_max.month, mth_max.day))]['logging_scr'].values)[0]

    except:
        last_1year_val = np.nan


    df_lst.append({'searchterm': search_tm, 'date_diff':(dt_max - dt_min).days, 'days30_diff': last_30_val,

        'quarter1_diff' : last_1quart_val,

        'quarter3_diff' : last_3quart_val,

        'year1_diff': last_1year_val})



###################

### History V2 ###

####################

season_terms, nonseason_term, error_terms


import calendar

def calculate_dt(dt_max, month_range):

    if dt_max.month-month_range<1:

        output_year = dt_max.year - 1
        output_month = dt_max.month - month_range + 12

    else:
        output_year = dt_max.year
        output_month = dt_max.month - month_range

    monthRange = calendar.monthrange(output_year, output_month)

    dt_output = datetime.date(output_year, output_month, monthRange[1])

    return dt_output

def get_val(df, search_tm, last_dt, dt_max):
    val1 = df[(df['searchterm']==search_tm) & (df['date']==last_dt)]['logging_scr'].values

    val2 = df[(df['searchterm']==search_tm) & (df['date']==dt_max)]['logging_scr'].values

    if len(val1)==0:
        val1 = 0
    if len(val2)==0:
        val2 = 0

    return [0] if val1==0 and val2==0 else val1-val2





df_lst = []

ind = 0
search_tm = search_trm_S[0]
for search_tm in search_trm_S:
    try:
        ind = ind+1
        print(ind)
        df_week_sel1 = df_weekP[(df_weekP['searchterm']==search_tm)]
        dt_max = df_week_sel1['date'].max()
        dt_min = df_week_sel1['date'].min()

        last_30 = dt_max + datetime.timedelta(days=-1*30)
        one_quarter = calculate_dt(dt_max, 3)
        three_quarter = calculate_dt(dt_max, 9)


        last_30_val = get_val(df_weekP, search_tm, last_30, dt_max)[0]

        last_1quart_val = get_val(df_weekP, search_tm, one_quarter, dt_max)[0]

        last_3quart_val = get_val(df_weekP, search_tm, three_quarter, dt_max)[0]

        last_1year_val = get_val(df_weekP, search_tm, datetime.date(dt_max.year-1, dt_max.month, calendar.monthrange(dt_max.year-1, dt_max.month)[1]), dt_max)[0]



        df_lst.append({'searchterm': search_tm, 'date_diff':(dt_max - dt_min).days, 'days30_diff': last_30_val,

            'latest_active':( datetime.date(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)  - dt_min).days,

            'quarter1_diff' : last_1quart_val,

            'quarter3_diff' : last_3quart_val,

            #'year1_diff': last_1year_val,

            'seasonality': seasonality(df_month_sel1)})
    except:
        print(search_tm)

df_chk = pd.DataFrame(df_lst)




Xs = df_chk.iloc[:,1:-2]

from sklearn.mixture import GaussianMixture
gmm = GaussianMixture(n_components=6).fit(Xs)
labels = gmm.predict(Xs)
print('GMM预测值',labels)
df_chk['labels'] = labels
from collections import Counter
Counter(labels)


df_plot = df_week[df_week['searchterm'].isin(df_chk['searchterm'].unique())].merge(df_chk,how='left', on = 'searchterm')
df_plot.to_csv('chk.csv')


df_chk[df_chk['seasonality']==True]['labels'].unique()

df_plot['标签定义'] = ''
df_plot.loc[df_plot['labels']==4,'标签定义'] = '衰退型需求'
df_plot.loc[df_plot['searchterm'].isin(stable_decrease),'标签定义'] = '稳定下降'

{'无增长':['smallest desk lamp', 'ultra wide led desk lamp', 'small desk ring light', 'ampulla small desk lampdesk lamp ampulla'],
'周期性稳定': stable_period= ['small desk lamps for home office', 'desk lamp for kids', 'small desk lamp', 'gray desk lamp',  'small desk lamps',  'small desk lamps for small spaces', 'desk lamp for college dorm room']
'周期性下降': periodic_decrease = ['desk lamp black']
'周期增长': increasing = ['aesthetic desk lamp', 'small lamp for desk']
'稳定增长' : [ 'clip on light for desk']
 '稳定': stale = ['desk lamp small', 'small ring light for desk', 'small desk light']
 '稳定下降': stable_decrease = ['desk lamp black','rose gold lamps']
 '潜力增长'： 'small touch lamps'
 'unkown':['small black desk lamp', 'simple desk lamp',
 'simple desk lamp', []
 'small led desk lamp' ] [delete later]

df_plot[df_plot['searchterm']=='smallest desk lamp']


df_plot.merge(df_chk[['searchterm', 'date_diff', 'days30_diff', 'latest_active',
       'quarter1_diff', 'quarter3_diff']], how='left', on ='searchterm').to_csv('chk.csv')
df_plot[df_plot['labels']==6]['searchterm'].unique()


df = pd.read_csv('./files/small desk lamp.csv')
df[df['搜索词'].str.contains('small')]
