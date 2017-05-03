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

noise_comp_dist_res = lines.map(lambda line: (line[23].encode('utf-8').strip().upper(),
					  1 if line[5].encode('utf-8').strip().upper() == "NOISE - RESIDENTIAL"  else 0)) \
				.reduceByKey(add)
    
    
noise_comp_dist_sw = lines.map(lambda line: (line[23].encode('utf-8').strip().upper(),
					  1 if line[5].encode('utf-8').strip().upper() == "NOISE - STREET/SIDEWALK"  else 0)) \
				.reduceByKey(add)
    
noise_comp_dist_veh = lines.map(lambda line: (line[23].encode('utf-8').strip().upper(),
					  1 if line[5].encode('utf-8').strip().upper() == "NOISE - VEHICLE"  else 0)) \
				.reduceByKey(add)
    
df_res = noise_comp_dist_res.toDF(["borough", "res_freq"])

df_sw = noise_comp_dist_sw.toDF(["borough", "sw_freq"])

df_veh = noise_comp_dist_veh.toDF(["borough", "veh_freq"])

crash = crash.map(lambda x: (x[2].encode('utf-8').strip().upper(), 1)) \
		.reduceByKey(add)

df_crash = crash.toDF(["borough", "crash_freq"])

df_res = df_res.join(df_crash, df_crash.borough == df_res.borough)
df_sw = df_sw.join(df_crash, df_crash.borough == df_sw.borough)
df_veh = df_veh.join(df_crash, df_crash.borough == df_veh.borough)

df_res.map(lambda x : "%s,%s,%s" % (x[0], x[1], x[3])) \
        .saveAsTextFile("res_noise_crash_boro.out")

df_sw.map(lambda x : "%s,%s,%s" % (x[0], x[1], x[3])) \
        .saveAsTextFile("sw_noise_crash_boro.out")

df_veh.map(lambda x : "%s,%s,%s" % (x[0], x[1], x[3])) \
        .saveAsTextFile("veh_noise_crash_boro.out")