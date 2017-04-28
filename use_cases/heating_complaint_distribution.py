from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from pyspark.sql import HiveContext

if len(sys.argv) != 2:
	print("Usage: heating_complaint_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
sqlContext = HiveContext(sc)
#sqlContext = SQLContext(sc)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

weather = sc.textFile('/user/ac5901/weather_data.csv', 1, use_unicode=False)
weather = weather.mapPartitions(lambda x: reader(x))

heating_complaint_dist = lines.map(lambda line: (datetime.strptime(line[1][0:10].encode('utf-8').strip(), "%m/%d/%Y"), 1 if line[5].encode('utf-8').strip().upper() in ['HEATING', 'HEAT/HOT WATER'] else 0)) \
				.reduceByKey(add)
#.sortBy(lambda x: (-x[1], x[0])) 
#.map(lambda x: "%s\t%s" % (x[0], x[1]))

#heating_complaint_dist.saveAsTextFile("heating_complaint_dist.out")

df_heating = heating_complaint_dist.toDF(["created_date", "frequency"])
df_heating.registerTempTable("df_heating")

quartile_1 = sqlContext.sql("SELECT percentile(frequency, 0.25) FROM df_heating").map(lambda x: x._c0).collect()[0]
quartile_3 = sqlContext.sql("SELECT percentile(frequency, 0.75) FROM df_heating").map(lambda x: x._c0).collect()[0]

lb = quartile_1 - 1.5 * (quartile_3 - quartile_1)
ub = quartile_3 + 1.5 * (quartile_3 - quartile_1)

#heating_complaints_filtered = heating_complaint_dist.filter(lambda x: (float(x[1]) > ub) or (float(x[1]) < lb)) \
#			.sortBy(lambda x: (-x[1], x[0]))
			

weather = weather.map(lambda x: (datetime.strptime(x[2].encode('utf-8').strip(), "%Y%m%d"), float(x[3].encode('utf-8').strip())))
df_weather = weather.toDF(["date", "temperature"])
df_weather.registerTempTable("df_weather")

df_joined = df_heating.join(df_weather, df_heating.created_date == df_weather.date) 

df_joined.map(lambda x: (x[0], x[1], x[3], 'unexpected' if ((float(x[1]) > ub) or (float(x[1]) < lb)) else 'expected')) \
	.sortBy(lambda x: (x[0])) \
	.map(lambda x: "%s,%s,%s,%s" % (x[0].strftime("%Y%m%d"), x[1], x[2], x[3])) \
	.saveAsTextFile("heating_complaints_temp.out")

corr_coeff = df_joined.stat.corr("frequency", "temperature")

minval = heating_complaint_dist.values().min()
maxval = heating_complaint_dist.values().max()
median = sqlContext.sql("SELECT percentile(frequency, 0.5) FROM df_heating").map(lambda x: x._c0).collect()[0]
avg = heating_complaint_dist.values().sum()/float(heating_complaint_dist.count())

l = [("min_num_complaints", minval), ("quartile_1_num_complaints", quartile_1), ("median_num_complaints", median), ("quartile_3_num_complaints", quartile_3), ("max_num_complaints",maxval), ("correlation_score_num_complaints_temp", corr_coeff)]
sc.parallelize(l) \
	.map(lambda x: "%s: %.2f" % (x[0], x[1])) \
	.saveAsTextFile("heating_complaints_temp_summary.out")
#	.map(lambda x: "Summary:\nmin_num_complaints: %.2f\nquartile_1_num_complaints: %.2f\nmedian_num_complaints: %.2f\nquartile_3_num_complaints: %.2f\nmax_num_complaints: %.2f\ncorrelation_score_num_complaints_temp: %.2f" % (x[0], x[1], x[2], x[3], x[4], x[5])) \
#	.saveAsTextFile("heating_complaints_temp_summary.out")

sc.stop()
