from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def get_day(val):
	if val == 0:
		return '1,Mo'
	elif val == 1:
		return '2,Tu'
	elif val == 2:
		return '3,We'
	elif val == 3:
		return '4,Th'
	elif val == 4:
		return '5,Fr'
	elif val == 5:
		return '6,Sa'
	elif val == 6:
		return '7,Su'
	else:
		return 'N/A'

def get_time(val, timestamp):
	if timestamp == "A" and val == "12":
		return "00"
	elif timestamp == "P":
		return val if val == "12" else str(int(val) + 12)
	else:
		return val

if len(sys.argv) != 2:
	print("Usage: day_time_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

hour_dist = lines.map(lambda line : (get_day(datetime.strptime(line[1].encode('utf-8'), "%m/%d/%Y %I:%M:%S %p").weekday()) + "," + get_time(line[1].strip()[11:13].encode('utf-8'), line[1].strip()[20:21].encode('utf-8')), 1)) \
            .reduceByKey(add) \
            .sortBy(lambda x: (x[0],x[1])) \
			.map(lambda x: "%s,%s" % (x[0], x[1])) \
			.saveAsTextFile("day_time_dist.out")
sc.stop()

