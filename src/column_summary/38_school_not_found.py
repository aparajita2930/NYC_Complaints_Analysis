from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader
import re

sc = SparkContext()
sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *


def check_entry(val):
	basetype = get_basetype(val)
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val in ['Unspecified', 'NA', 'N/A']:
			return 'NULL'
		elif val.strip().upper() in ['N', 'Y', 'NO', 'YES']:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, val1, basetype):
	chk = check_entry(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'school_not_found_indicator'
	else:
		return 'None'

def col_details(val0, val1):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, val1, basetype)
	validity = check_entry(val0)
	return (val0, basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 38_school_not_found <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))

details = lines.map(lambda line : (line[37].encode('utf-8').strip(), 1)) \
 			.reduceByKey(add) \
			.map(lambda x: col_details(x[0], x[1]))

details.map(lambda x: "%s\t%s %s %s" % (x[0], x[1], x[2], x[3])).saveAsTextFile("38_school_not_found.out")

sc.stop()