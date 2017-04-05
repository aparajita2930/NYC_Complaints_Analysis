import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if len(sys.argv) != 3:
	print "Usage: join_csvs.py <file1-2010_2016> <file2-2009>"
	exit(-1)
sc = SparkContext()
lines_2010_2016 = sc.textFile(sys.argv[1], 1)
lines_2009 = sc.textFile(sys.argv[2], 1)
lines_2009_header = lines_2009.first()

lines_2009 = lines_2009.filter(lambda x: x != lines_2009_header)

#lines_2009 = lines_2009.mapPartitionsWithIndex(lambda (idx, iter): (iter.drop(1) if (idx == 0) else iter))

#lines_all = lines_2010_2016.unionAll(lines_2009)
lines_2010_2016.union(lines_2009).coalesce(1).saveAsTextFile("NYC_Complaints.csv")

sc.stop()

