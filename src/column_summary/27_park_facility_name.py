'''
Source:
https://en.wikipedia.org/wiki/List_of_New_York_City_parks
'''

from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *
park_lines = sc.textFile("/user/ac5901/park.csv",1)
parks = park_lines.map(lambda x: x).collect()

def check_park(val):
	basetype = get_basetype(val)
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val in ['Unspecified', 'NA', 'N/A', 'N?A', 'NA/']:
			return 'NULL'
		elif val.upper() in parks:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, basetype):
	chk = check_park(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'park'
	else:
		return 'None'

def col_details(val0):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, basetype)
	validity = check_park(val0)
	return "%s %s %s" % (basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 27_park_facility_name <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[26].encode('utf-8').strip(), col_details(line[26].encode('utf-8').strip()))))
details.saveAsTextFile("27_details.out")

sc.stop()
