from __future__ import print_function

import sys,re
from operator import add
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
		print("Usage: 0_create_file_summary.py <file>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	filename = sys.argv[1]
	lines = sc.textFile(filename + "/part*", 1)
	lines = lines.mapPartitions(lambda x: reader(x, delimiter='\t'))
	counts = lines \
	     .map(lambda x: ("%s, %s" % x[1],1)) \
	     .reduceByKey(lambda x,y: x + y) \
	     .map(lambda (x,y): "%s\t%s" % (x,y))
	saveFileName = filename.replace("_details.out","_summary.out")
	counts.saveAsTextFile(saveFileName)
	sc.stop()