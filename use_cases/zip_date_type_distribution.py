from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if len(sys.argv) != 2:
	print("Usage: complaint_type_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x)) 
date_zip_complaint_type_dist = lines.map(lambda line : ((line[1].strip().encode('utf-8'),line[8].encode('utf-8'), line[5].encode('utf-8')), 1)) \
				.reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0][0], x[0][1],x[0][2])) \
				.map(lambda x: "%s, %s, %s\t%s" % (x[0][0], x[0][1], x[0][2], x[1]))
date_zip_complaint_type_dist.saveAsTextFile("date_zip_complaint_type_dist.out")