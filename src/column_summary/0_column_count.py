from __future__ import print_function

import sys,re
from operator import add
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: 0_column_count <file> <column_number>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
	lines = lines.mapPartitions(lambda x: reader(x))
	column_number = int(sys.argv[2])
	counts = lines \
	     .map(lambda x: (x[column_number].encode('utf-8').strip(),1)) \
	     .reduceByKey(lambda x,y: x + y) \
	     .sortBy(lambda x:x[1]) \
	     .map(lambda (x,y): "%s\t%s" % (x,y))
	filename = sys.argv[2] + "_column_count.out"
	counts.saveAsTextFile(filename)
	sc.stop()