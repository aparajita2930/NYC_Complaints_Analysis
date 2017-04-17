from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
sc.addFile("src/helper/assign_basetype.py")
from assign_basetype import *

'''def split_tuple(full_tuple, l):
    indv = [iter(full_tuple)] * l
    return zip(*indv)'''

def get_semantictype(val0, val1, basetype):
	if basetype == 'INT' and len(str(val0)) == 8:
		return 'key'
	else:
		return 'text'

def get_validity(val0, val1, basetype):
	if basetype == 'TEXT':
		if val is None or len(val0.strip()) == 0:
			return (val0, 'NULL')
		else:
			return (val0, 'INVALID')
	elif basetype == 'INT':
		if val1 == 1:
			return (val0, 'VALID')
		else:
			return (val0, 'INVALID')
	#elif val1 > 1:
	#	return split_tuple((val0, 'INVALID')*val1, 2)
	else:
		return (val0, 'INVALID')

def col_details(val0, val1):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, val1, basetype)
	return (val0, basetype, semantictype, get_validity(val0, val1, basetype)[1])

if len(sys.argv) != 2:
	print("Usage: 1_unique_key <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))

details_unique_key = lines.map(lambda line : (line[0].encode('utf-8').strip(), 1))

details_unique_key.map(lambda x: "%s\t%s %s %s" % (x[0], x[1], x[2], x[3])).saveAsTextFile("1_details.out")

sc.stop()
