import pandas as pd
df_short = pd.DataFrame(df_lst_short)
Xs_short = df_short[['距今多久未上榜', '平均上榜间隔', '本季度排名', '上季度排名', '平均年度变化', '上周排名']]

from sklearn.mixture import GaussianMixture
gmm = GaussianMixture(n_components=3).fit(Xs_short)
labels = gmm.predict(Xs_short)
print('GMM预测值',labels)
Xs_short['labels'] = labels
from collections import Counter
Counter(labels)
df_short['labels'] = labels
# df_short.to_csv('chk_clusterSHORT.csv')


df_long = pd.DataFrame(df_lst_long)
Xs_long = df_long[['active_dt_range', 'latest_active', '环比增长趋势', 'Last Year 年度增长','去年同期分值', '近期分值']].fillna(0)


from sklearn.mixture import GaussianMixture
from sklearn.cluster import KMeans

kmean = KMeans(n_clusters=3).fit(Xs_long)
labels = kmean.predict(Xs_long)
df_long['labels'] = labels
print('GMM预测值',labels)

from collections import Counter
Counter(labels)
#

# df_long.to_csv('chk_clusterLONG.csv')



########
