from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def get_day(val):
	if val == 0:
		return 'Monday'
	elif val == 1:
		return 'Tuesday'
	elif val == 2:
		return 'Wednesday'
	elif val == 3:
		return 'Thursday'
	elif val == 4:
		return 'Friday'
	elif val == 5:
		return 'Saturday'
	elif val == 6:
		return 'Sunday'
	else:
		return 'N/A'

if len(sys.argv) != 2:
	print("Usage: complaint_type_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

overall_complaint_type_dist = lines.map(lambda line: (line[5].encode('utf-8').strip(), 1)) \
				.reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0])) \
				.map(lambda x: "%s\t%s" % (x[0], x[1]))

year_dist = lines.map(lambda line : (line[1].strip()[6:10].encode('utf-8') if len(str(line[1]).strip()) == 22 else None, 1)) \
			.reduceByKey(add) \
			.sortBy(lambda x: (-x[1], x[0])) \
			.map(lambda x: "%s\t%s" % (x[0], x[1]))


year_complaint_type_dist = lines.map(lambda line : ((line[1].strip()[6:10].encode('utf-8'), line[5].encode('utf-8')) if len(str(line[1]).strip()) == 22 else (None, line[5].encode('utf-8')), 1)) \
				.reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
				.map(lambda x: "%s, %s\t%s" % (x[0][0], x[0][1], x[1]))



hour_dist = lines.map(lambda line : (line[1].strip()[11:13].encode('utf-8')+line[1].strip()[20:22].encode('utf-8') if len(str(line[1]).strip()) == 22 else None, 1)) \
                        .reduceByKey(add) \
			.sortBy(lambda x: (-x[1], x[0])) \
                        .map(lambda x: "%s\t%s" % (x[0], x[1]))


hour_complaint_type_dist = lines.map(lambda line : ((line[1].strip()[11:13].encode('utf-8')+line[1].strip()[20:22].encode('utf-8'), line[5].encode('utf-8')) if len(str(line[1]).strip()) == 22 else (None, line[5].encode('utf-8')), 1)) \
                                .reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
                                .map(lambda x: "%s, %s\t%s" % (x[0][0], x[0][1], x[1]))


day_dist = lines.map(lambda line : (get_day(datetime.strptime(line[1].encode('utf-8'), "%m/%d/%Y %I:%M:%S %p").weekday()), 1)) \
                        .reduceByKey(add) \
			.sortBy(lambda x: (-x[1], x[0])) \
                        .map(lambda x: "%s\t%s" % (x[0], x[1]))


day_complaint_type_dist = lines.map(lambda line : ((get_day(datetime.strptime(line[1].encode('utf-8'), "%m/%d/%Y %I:%M:%S %p").weekday()), line[5].encode('utf-8')), 1)) \
                                .reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
                                .map(lambda x: "%s, %s\t%s" % (x[0][0], x[0][1], x[1]))


         
      
overall_complaint_type_dist.saveAsTextFile("overall_complaint_type_dist.out")                    
year_dist.saveAsTextFile("year_dist.out")
year_complaint_type_dist.saveAsTextFile("year_complaint_type_dist.out")
hour_dist.saveAsTextFile("hour_dist.out")
hour_complaint_type_dist.saveAsTextFile("hour_complaint_type_dist.out")
day_dist.saveAsTextFile("day_dist.out")
day_complaint_type_dist.saveAsTextFile("day_complaint_type_dist.out")

sc.stop()
