from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if len(sys.argv) != 2:
	print("Usage: zip_date_type_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x)) 
date_zip_complaint_type_dist = lines.map(lambda line : ((line[1].strip().encode('utf-8'),line[8].encode('utf-8'), line[5].encode('utf-8')), 1)) \
				.filter(lambda ((d,z,t),c) : z and 'HEAT' not in t) \
				.reduceByKey(lambda x,y: x + y) \
				.sortBy(lambda x:x[1], False) \
				.take(500)
				#.map(lambda x: "%s, %s, %s\t%s" % (x[0][0], x[0][1], x[0][2], x[1]))
count = sc.parallelize(date_zip_complaint_type_dist)
date_zip_complaint_type_dist = count.map(lambda (x,y): "%s\t%s" % (x,y))
date_zip_complaint_type_dist.saveAsTextFile("date_zip_complaint_type_dist.out")
