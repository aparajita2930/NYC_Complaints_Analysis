from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *
all_lines = sc.textFile("/user/ac5901/ferry_terminal_name.csv",1)
valid_types = all_lines.map(lambda x: x).collect()

def get_label(val, basetype):
	val = val.upper()
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val in ['UNSPECIFIED', 'NA', 'N/A', 'N?A', 'NA/','NONE', 'UNKNOWN']:
			return 'NULL'
		elif val in valid_types:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(basetype, label):
	if label == 'NULL' or label == 'VALID':
		return 'ferry_terminal_name'
	else:
		return 'NONE'

def create_labels(value):
	basetype = get_basetype(value)
	label = get_label(value, basetype)
	semantictype = get_semantictype(basetype, label)
	return "%s %s %s" % (basetype, semantictype, label)

if len(sys.argv) != 2:
	print("Usage: 49_ferry_terminal_name <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[48].strip(), create_labels(line[48].strip()))))
details.saveAsTextFile("49_details.out")
sc.stop()
