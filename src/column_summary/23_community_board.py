from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *
cb_lines = sc.textFile("/user/ac5901/community_board.csv",1)
cb = cb_lines.map(lambda x: x).collect()

def check_cb(val):
	basetype = get_basetype(val)
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val in ['Unspecified', 'NA', 'N/A', 'N?A', 'NA/'] or val.find('Unspecified') <> -1:
			return 'NULL'
		elif val.upper() in cb:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, basetype):
	chk = check_cb(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'community_board'
	else:
		return 'None'

def col_details(val0):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, basetype)
	validity = check_cb(val0)
	return "%s %s %s" % (basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 23_community_board <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[22].encode('utf-8').strip(), col_details(line[22].encode('utf-8').strip()))))
details.saveAsTextFile("23_details.out")

sc.stop()
