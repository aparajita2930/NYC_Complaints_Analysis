'''
Source of school list:
http://schools.nyc.gov/schoolsearch/
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
school_lines = sc.textFile("/user/ac5901/school_code.csv",1)
school_codes = school_lines.map(lambda x: x).collect()

def check_school(val):
	basetype = get_basetype(val)
	if basetype == 'TEXT' or basetype == 'INT':
		if val is None or len(str(val).strip()) == 0 or str(val) in ['Unspecified', 'NA', 'N/A', 'N?A', 'NA/']:
			return 'NULL'
		elif str(val) in school_codes:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, basetype):
	chk = check_school(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'school_code'
	else:
		return 'None'

def col_details(val0):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, basetype)
	validity = check_school(val0)
	return "%s %s %s" % (basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 32_school_code <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[31].encode('utf-8').strip(), col_details(line[31].encode('utf-8').strip()))))
details.saveAsTextFile("32_details.out")
sc.stop()
