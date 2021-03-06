'''
Source of school regions:
https://en.wikipedia.org/wiki/List_of_public_elementary_schools_in_New_York_City
http://www.newyorkschools.com/districts/nyc-district-75.html
http://schools.nyc.gov/Offices/District79/default.htm
'''
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


def check_region(val):
	basetype = get_basetype(val)
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val in ['Unspecified', 'NA', 'N/A']:
			return 'NULL'
		elif val.strip().upper() in ['REGION 1', 'REGION 2', 'REGION 3', 'REGION 4', 'REGION 5', 'REGION 6', 'REGION 7', 'REGION 8', 'REGION 9', 'REGION 10', 'DISTRICT 75', 'DISTRICT 79']:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, basetype):
	chk = check_region(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'school_region'
	else:
		return 'None'

def col_details(val0):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, basetype)
	validity = check_region(val0)
	return "%s %s %s" % (basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 31_school_region <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[30].encode('utf-8').strip(), col_details(line[30].encode('utf-8').strip()))))
details.saveAsTextFile("31_details.out")

sc.stop()
