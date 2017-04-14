from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def check_date(val):
	if len(val) == 22:
		yr = 0
		try:
			yr = int(val[6:10])
		except ValueError:
			pass
		if yr >= 2009 and yr <= 2017:
			return True
		else:
			return False
	else:
		return False

if len(sys.argv) != 2:
	print("Usage: closing_time_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

overall_closing_time_dist = lines.map(lambda line: ((datetime.strptime(line[2].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p") - datetime.strptime(line[1].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p")).days if check_date(line[2].encode('utf-8').strip()) else 0, 1)) \
				.reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0])) \
				.map(lambda x: "%s\t%s" % (x[0], x[1]))


agency = lines.map(lambda line: (line[3].encode('utf-8').strip(), 1))
agency_count = sc.broadcast(agency.countByKey())

agency_closing_time_dist = lines.map(lambda line: (line[3].encode('utf-8').strip(), (datetime.strptime(line[2].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p") - datetime.strptime(line[1].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p")).days if check_date(line[2].encode('utf-8').strip()) else 0)) \
				.reduceByKey(add) \
				.map(lambda x: (x[0], x[1]/float(agency_count.value[x[0]]))) \
				.sortBy(lambda x: (-x[1], x[0])) \
				.map(lambda x: "%s\t%s" % (x[0], x[1]))




complaint = lines.map(lambda line: (line[5].encode('utf-8').strip(), 1))
complaint_count = sc.broadcast(complaint.countByKey())

complaint_closing_time_dist = lines.map(lambda line: (line[5].encode('utf-8').strip(), (datetime.strptime(line[2].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p") - datetime.strptime(line[1].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p")).days if check_date(line[2].encode('utf-8').strip()) else 0)) \
                                .reduceByKey(add) \
                                .map(lambda x: (x[0], x[1]/float(complaint_count.value[x[0]]))) \
                                .sortBy(lambda x: (-x[1], x[0])) \
                                .map(lambda x: "%s\t%s" % (x[0], x[1]))



city = lines.map(lambda line: (line[16].encode('utf-8').strip(), 1))
city_count = sc.broadcast(city.countByKey())

city_closing_time_dist = lines.map(lambda line: (line[16].encode('utf-8').strip(), (datetime.strptime(line[2].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p") - datetime.strptime(line[1].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p")).days if check_date(line[2].encode('utf-8').strip()) else 0)) \
                                .reduceByKey(add) \
                                .map(lambda x: (x[0], x[1]/float(city_count.value[x[0]]))) \
                                .sortBy(lambda x: (-x[1], x[0])) \
                                .map(lambda x: "%s\t%s" % (x[0], x[1]))




borough = lines.map(lambda line: (line[23].encode('utf-8').strip(), 1))
borough_count = sc.broadcast(borough.countByKey())

borough_closing_time_dist = lines.map(lambda line: (line[23].encode('utf-8').strip(), (datetime.strptime(line[2].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p") - datetime.strptime(line[1].encode('utf-8').strip(), "%m/%d/%Y %I:%M:%S %p")).days if check_date(line[2].encode('utf-8').strip()) else 0)) \
                                .reduceByKey(add) \
                                .map(lambda x: (x[0], x[1]/float(borough_count.value[x[0]]))) \
                                .sortBy(lambda x: (-x[1], x[0])) \
                                .map(lambda x: "%s\t%s" % (x[0], x[1]))


      
overall_closing_time_dist.saveAsTextFile("overall_closing_time_dist.out")                    
agency_closing_time_dist.saveAsTextFile("agency_closing_time_dist.out")
complaint_closing_time_dist.saveAsTextFile("complaint_closing_time_dist.out")
city_closing_time_dist.saveAsTextFile("city_closing_time_dist.out")
borough_closing_time_dist.saveAsTextFile("borough_closing_time_dist.out")

sc.stop()
