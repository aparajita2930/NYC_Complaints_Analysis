from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

if len(sys.argv) != 2:
	print("Usage: 0_column_summary <column_number>",  file=sys.stderr)
	exit(-1)
idx = sys.argv[1]
filename = idx + "_details.out"
lines = sc.textFile(filename, 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x, delimiter='\t')) \
		.map(lambda x: x[1])
		
values = lines.mapPartitions(lambda x: reader(x, delimiter=' '))

datatype = values.map(lambda x: (x[0].encode('utf-8').strip(), 1)) \
			.reduceByKey(add) \
			.map(lambda x: "%s\t%s" % (x[0], x[1]))


semantictype = values.map(lambda x: (x[1].encode('utf-8').strip(), 1)) \
                        .reduceByKey(add) \
                        .map(lambda x: "%s\t%s" % (x[0], x[1]))


validity = values.map(lambda x: (x[2].encode('utf-8').strip(), 1)) \
                        .reduceByKey(add) \
                        .map(lambda x: "%s\t%s" % (x[0], x[1]))


datatype.saveAsTextFile(idx+"_datatype.out")
semantictype.saveAsTextFile(idx + "_semantictype.out")
validity.saveAsTextFile(idx + "_validity.out")

sc.stop()
