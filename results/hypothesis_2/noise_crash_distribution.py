from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from pyspark.sql import HiveContext

if len(sys.argv) != 2:
	print("Usage: noise_crash_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
sqlContext = HiveContext(sc)

lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))

crash = sc.textFile("/user/sar516/collisions.csv", 1, use_unicode=False)
crash = crash.filter(lambda line: "DATE" not in line)
crash = crash.mapPartitions(lambda x: reader(x))

noise_comp_dist = lines.map(lambda line: (datetime.strptime(line[1][0:10].encode('utf-8').strip(), "%m/%d/%Y"),
					  1 if line[5].encode('utf-8').strip() == "Noise - Residential"  else 0)) \
				.reduceByKey(add)


df_noise = noise_comp_dist.toDF(["created_date", "comp_freq"])
df_noise.registerTempTable("df_noise")

quartile_1 = sqlContext.sql("SELECT percentile(comp_freq, 0.25) FROM df_noise").map(lambda x: x._c0).collect()[0]
quartile_3 = sqlContext.sql("SELECT percentile(comp_freq, 0.75) FROM df_noise").map(lambda x: x._c0).collect()[0]

lb = quartile_1 - 1.5 * (quartile_3 - quartile_1)
ub = quartile_3 + 1.5 * (quartile_3 - quartile_1)

crash = crash.map(lambda x: (datetime.strptime(x[0].encode('utf-8').strip(), "%m/%d/%Y"), 1)) \
		.reduceByKey(add)
df_crash = crash.toDF(["date", "crash_freq"])
df_crash.registerTempTable("df_crash")

df_joined = df_noise.join(df_crash, df_noise.created_date == df_crash.date)

df_joined.map(lambda x: (x[0], x[1], x[3], 'unexpected' if ((float(x[1]) > ub) or (float(x[1]) < lb)) else 'expected')) \
	.sortBy(lambda x: (x[0])) \
	.map(lambda x: "%s,%s,%s,%s" % (x[0].strftime("%Y%m%d"), x[1], x[2], x[3])) \
	.saveAsTextFile("noise_complaints_crash.out")

corr_coeff = df_joined.stat.corr("comp_freq", "crash_freq")

minval = noise_comp_dist.values().min()
maxval = noise_comp_dist.values().max()
median = sqlContext.sql("SELECT percentile(comp_freq, 0.5) FROM df_noise").map(lambda x: x._c0).collect()[0]
mean = noise_comp_dist.values().sum()/float(noise_comp_dist.count())

l = [("min_num_complaints", minval), ("quartile_1_num_complaints", quartile_1), ("median_num_complaints", median), ("quartile_3_num_complaints", quartile_3), ("max_num_complaints",maxval), ("correlation_score_num_complaints_crash", corr_coeff)]
sc.parallelize(l) \
	.map(lambda x: "%s: %.2f" % (x[0], x[1])) \
	.saveAsTextFile("noise_complaints_crash_summary.out")
