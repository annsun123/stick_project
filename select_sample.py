import random

import pandas as pd
import pymysql
import numpy as np
import datetime, calendar

### chk all categories ##
db = pymysql.connect(host='106.13.219.126', port=4000,
                     user='tidb_analysis', password='rlqf23mLnohuAlHT', database='brand_analysis')
cursor = db.cursor()

try:
    select1 = """ select distinct category_level1 from brand_analysis.quarter where domain='amazon.com' """
    cursor.execute(select1)
    result2 = cursor.fetchall()
    cat_lst = [x[0] for x in result2]
     #   desc = cursor.description
     #   df_result= pd.DataFrame(result2)
     #   df_result.columns = [x[0] for x in desc]
except:
    print('error')

### call all data from categories


for cat in cat_lst:

    db = pymysql.connect(host='106.13.219.126', port=4000,
                         user='tidb_analysis', password='rlqf23mLnohuAlHT', database='brand_analysis')
    cursor = db.cursor()

    try:

        select1 = """ select * from brand_analysis.quarter where domain='amazon.com' and category_level1 = "%s" """
        cursor.execute(select1%cat)
        result2 = cursor.fetchall()
        desc = cursor.description
        df_cat = pd.DataFrame(result2)
        df_cat.columns = [x[0] for x in desc]

    except:
        print('error')


    cat_sel = {}

    df_sel = df_cat[df_cat['category_level1']==cat]

    search_trm1 = [list(df_sel[(df_sel['searchfrequencyrank']<=10000)]['searchterm'].unique())\
         [random.randint(0, len(list(df_sel[(df_sel['searchfrequencyrank']<=10000)]\
                                         ['searchterm'].unique()))-1)] for _ in range(50)]

    search_trm2 = [list(df_sel[(df_sel['searchfrequencyrank']>10000) & \
                                      (df_sel['searchfrequencyrank']<100000)]['searchterm'].unique())\
         [random.randint(0, len(list(df_sel[(df_sel['searchfrequencyrank']>10000) & \
                                      (df_sel['searchfrequencyrank']<100000)]\
                                         ['searchterm'].unique()))-1)] for _ in range(50)]

    search_trm3 = [list(df_sel[(df_sel['searchfrequencyrank']>100000) & (df_sel['searchfrequencyrank']<500000)]['searchterm'].unique())\
         [random.randint(0, len(list(df_sel[(df_sel['searchfrequencyrank']>100000) & (df_sel['searchfrequencyrank']<500000)]\
                                         ['searchterm'].unique()))-1)] for _ in range(50)]

    search_trm4 = [list(df_sel[(df_sel['searchfrequencyrank']>500000) & (df_sel['searchfrequencyrank']<1000000)]['searchterm'].unique())\
         [random.randint(0, len(list(df_sel[(df_sel['searchfrequencyrank']>500000) & (df_sel['searchfrequencyrank']<1000000)]\
                                         ['searchterm'].unique()))-1)] for _ in range(50)]

    search_trm5 = [list(df_sel[(df_sel['searchfrequencyrank']>1000000) & (df_sel['searchfrequencyrank']<2000000)]['searchterm'].unique())\
         [random.randint(0, len(list(df_sel[(df_sel['searchfrequencyrank']>1000000) & (df_sel['searchfrequencyrank']<2000000)]\
                                         ['searchterm'].unique()))-1)] for _ in range(50)]


    all_terms = search_trm1 + search_trm2 + search_trm3 + search_trm4 + search_trm5



df_month_sample = pd.DataFrame()
for search_tm in all_terms:

    db = pymysql.connect(host='106.13.219.126', port=4000,
                         user='tidb_analysis', password='rlqf23mLnohuAlHT', database='spider')
    cursor = db.cursor()

    try:

        select1 = """ select * from spider.aba_monthly where domain='amazon.com' and searchterm = "%s" """
        cursor.execute(select1%search_tm)
        result2 = cursor.fetchall()
        desc = cursor.description
        df_temp = pd.DataFrame(result2)
        df_temp.columns = [x[0] for x in desc]
        df_month_sample = df_month_sample.append(df_temp)

    except:

        print('error')

