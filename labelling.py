"""

Index(['searchterm', 'active_dt_range', 'latest_active', '环比增长趋势',
       'Last Year 年度增长', '近期分值', '近期分值排名', '去年同期分值', '去年同期排名', '过去240scr',
       '过去240rank', 'seasonality', 'labels'],
      dtype='object')

"""
import numpy as np

label_index = df_long.groupby('labels')['latest_active'].valu


val = temp_label['Last Year 年度增长'].mean()
def give_content(val):
    if np.abs(val)<=0.25:
        content = 'little'
    elif np.abs(val)>0.25 and np.abs(val)<0.5:
        content = 'medium'
    else:
        content = 'high'

    return content

for label in range(3):

    temp_label = df_long[df_long['labels']==label]

    print({'label': label,
         '距去年同期增长幅度': give_content(temp_label['Last Year 年度增长'].mean())})



"""

### 指标
距今多久未上榜': (np.ceil(np.percentile(temp_label['latest_active'].values, 85))),
         '距去年同期增长幅度': temp_label['近期分值'].mean() -  temp_label['去年同期分值'].mean(),
         '上年平均月增长': temp_label['Last Year 年度增长'].mean(),
        '环比增长': temp_label['环比增长趋势'].mean() 
        
# 也不能用上升下降稳定来形容

# 增长，增长幅度 【高增长，低增长】
# 下降， 【短期内快速下降，短期内下降】
<=25 微小
25<val<=50 中等
val>75 高频

"""




