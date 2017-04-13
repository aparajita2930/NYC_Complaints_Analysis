from __future__ import print_function

import sys, os
from operator import add
from pyspark import SparkContext
from csv import reader

def get_label(value, basetype):
	if basetype == "TEXT":
		if not value:
			return "NULL"
		else:
			return "INVALID"
	elif (basetype == "DATETIME" and date_in_valid_time_range(value)):
		return "VALID"
	else:
		return "INVALID"

def get_semantics(basetype, label):
	if basetype == "DATETIME" and label == "VALID":
		return "CREATED_DATE"
	return "TEXT"

def create_labels(value):
	basetype = get_basetype(value)
	label = get_label(value, basetype)
	semantictype = get_semantics(basetype,label)
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: 2_created_date.py <file>",  file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	sc.addFile("src/helper/assign_basetype.py")
	sc.addFile("src/helper/date_common.py")
	from assign_basetype import *
	from date_common import *
	lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
	lines = lines.mapPartitions(lambda x: reader(x)) 
	details_created_date = lines.map(lambda line : ("%s\t%s" % (line[1].encode('utf-8').strip(), create_labels(line[1].encode('utf-8').strip()))))
	details_created_date.saveAsTextFile("2_details.out")
	sc.stop()