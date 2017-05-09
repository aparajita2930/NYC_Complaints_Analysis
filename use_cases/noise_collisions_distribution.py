from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from pyspark.sql import HiveContext

def split_key(key):
	arr = key.split(',')
	return arr[0], arr[1]

if len(sys.argv) != 2:
	print("Usage: noise_collisions_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
sqlContext = HiveContext(sc)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x)) 

sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *
zip_lines = sc.textFile("/user/ac5901/zip.csv",1)
zips = dict(zip_lines.mapPartitions(lambda x: reader(x)).collect())

def check_zip(val):
	basetype = get_basetype(val)
	pattern=re.compile('^(\-?[\d+]{5}(-[\d+]{4})?)$')
	if basetype == 'INT':
		if val in zips.keys():
			return 'VALID'
		else:
			return 'INVALID'
	elif basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val == 'Unspecified':
			return 'NULL'
		elif pattern.match(val):
			if val.strip()[0:val.strip().find('-')] in zips.keys():
				return 'VALID'
			else:
				return 'INVALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'
   

collisions = sc.textFile('/user/ac5901/collisions.csv', 1, use_unicode=False)
collisions = collisions.mapPartitions(lambda x: reader(x))
collisions = collisions.map(lambda x: (x[0].encode('utf-8').strip() + "," + x[3].encode('utf-8').strip(), 1)).filter(lambda x: check_zip(x[0].split(",")[1].strip()) == "VALID").reduceByKey(add)
df_collisions = collisions.toDF(["key", "collisions_count"])
df_collisions.registerTempTable("df_collisions")

# Complaint types Combined

noise_complaint_dist = lines.map(lambda line: (line[1][0:10].encode('utf-8').strip() + "," + line[8].encode('utf-8').strip().upper(), 1 if ("NOISE - VEHICLE" in line[5].encode('utf-8').strip().upper() and check_zip(line[8].encode('utf-8').strip()) == "VALID") else 0)) \
				.reduceByKey(add)


df_noise = noise_complaint_dist.toDF(["key", "frequency"])
df_noise.registerTempTable("df_noise")

quartile_1 = sqlContext.sql("SELECT percentile(frequency, 0.25) FROM df_noise").map(lambda x: x._c0).collect()[0]
quartile_3 = sqlContext.sql("SELECT percentile(frequency, 0.75) FROM df_noise").map(lambda x: x._c0).collect()[0]

lb = quartile_1 - 1.5 * (quartile_3 - quartile_1)
ub = quartile_3 + 1.5 * (quartile_3 - quartile_1)
		
df_joined = df_noise.join(df_collisions, df_noise.key == df_collisions.key) 

df_joined.map(lambda x: (x[0], x[1], x[3], 'unexpected' if ((float(x[1]) > ub) or (float(x[1]) < lb)) else 'expected')) \
	.sortBy(lambda x: (x[0])) \
	.map(lambda x: "%s,%s,%s,%s" % (x[0], x[1], x[2], x[3])) \
	.saveAsTextFile("noise_complaint_dist.out")

corr_coeff = df_joined.stat.corr("frequency", "collisions_count")

minval = noise_complaint_dist.values().min()
maxval = noise_complaint_dist.values().max()
median = sqlContext.sql("SELECT percentile(frequency, 0.5) FROM df_noise").map(lambda x: x._c0).collect()[0]
avg = noise_complaint_dist.values().sum()/float(noise_complaint_dist.count())

df_filtered = df_joined.map(lambda x: (x[0], x[1], x[3], 'unexpected' if ((float(x[1]) > ub) or (float(x[1]) < lb)) else 'expected')) \
        .sortBy(lambda x: (x[0])) \
	.filter(lambda x: x[3] == "unexpected")

df_filtered = df_filtered.toDF(["key", "frequency", "collisions_count", "status"])
df_filtered.registerTempTable("df_filtered")
corr_coeff_filtered = df_filtered.stat.corr("frequency", "collisions_count")


l = [("min_num_complaints", minval), ("quartile_1_num_complaints", quartile_1), ("median_num_complaints", median), ("quartile_3_num_complaints", quartile_3), ("max_num_complaints",maxval), ("correlation_score_num_noise_collisions", corr_coeff), ("correlation_score_fringe_num_noise_collisions", corr_coeff_filtered)]
sc.parallelize(l) \
	.map(lambda x: "%s: %.2f" % (x[0], x[1])) \
	.saveAsTextFile("noise_complaint_dist_summary.out")

sc.stop()

