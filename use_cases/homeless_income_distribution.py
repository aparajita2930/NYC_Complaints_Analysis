from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from pyspark.sql import HiveContext

if len(sys.argv) != 2:
	print("Usage: homeless_income_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
sqlContext = HiveContext(sc)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

income = sc.textFile('/user/ac5901/income_housing.csv', 1, use_unicode=False)
income = income.mapPartitions(lambda x: reader(x))

homeless_complaint_dist = lines.map(lambda line: (line[1][6:10].encode('utf-8').strip() + "," + line[23].encode('utf-8').strip().upper(), 1 if 'HOMELESS' in line[5].encode('utf-8').strip().upper() else 0)) \
				.reduceByKey(add)

df_homeless = homeless_complaint_dist.toDF(["key", "frequency"])
df_homeless.registerTempTable("df_homeless")

quartile_1 = sqlContext.sql("SELECT percentile(frequency, 0.25) FROM df_homeless").map(lambda x: x._c0).collect()[0]
quartile_3 = sqlContext.sql("SELECT percentile(frequency, 0.75) FROM df_homeless").map(lambda x: x._c0).collect()[0]
		

income = income.map(lambda x: (x[0].encode('utf-8').strip() + "," + x[1].encode('utf-8').strip().upper(), float(x[3].encode('utf-8').strip())))
df_income = income.toDF(["key", "income_ratio"])
df_income.registerTempTable("df_income")

df_joined = df_homeless.join(df_income, df_homeless.key == df_income.key) 

corr_coeff = df_joined.stat.corr("frequency", "income_ratio")

minval = homeless_complaint_dist.values().min()
maxval = homeless_complaint_dist.values().max()
median = sqlContext.sql("SELECT percentile(frequency, 0.5) FROM df_homeless").map(lambda x: x._c0).collect()[0]
avg = homeless_complaint_dist.values().sum()/float(homeless_complaint_dist.count())

l = [("min_num_complaints", minval), ("quartile_1_num_complaints", quartile_1), ("median_num_complaints", median), ("quartile_3_num_complaints", quartile_3), ("max_num_complaints",maxval), ("correlation_score_num_homeless_income", corr_coeff)]
sc.parallelize(l) \
	.map(lambda x: "%s: %.2f" % (x[0], x[1])) \
	.saveAsTextFile("homeless_complaint_dist_summary.out")

sc.stop()
