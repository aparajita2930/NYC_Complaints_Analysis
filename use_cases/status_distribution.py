from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if len(sys.argv) != 2:
	print("Usage: status_distribution <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    

status_dist = lines.map(lambda line: (line[19].encode('utf-8').strip(), 1)) \
				.reduceByKey(add) \
				.sortBy(lambda x: (-x[1], x[0])) \
				.map(lambda x: "%s\t%s" % (x[0], x[1]))


status_dist.saveAsTextFile("status_dist.out")

sc.stop()
