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

def check_phone(val):
	basetype = get_basetype(val)
	pattern=re.compile('^\(*(\-?[\d+]{3}(\)*-*\s*[\d+]{3}(-*[\d]{4}))?)$')
	if basetype == 'TEXT' or basetype == 'INT':
		if val is None or len(str(val).strip()) == 0 or str(val) in ['Unspecified', 'NA', 'N/A', 'NA 0/0']:
			return 'NULL'
		elif pattern.match(val):
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'

def get_semantictype(val0, basetype):
	chk = check_phone(val0)
	if chk == 'NULL' or chk == 'VALID':
		return 'school_phone_number'
	else:
		return 'None'

def col_details(val0):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, basetype)
	validity = check_phone(val0)
	return "%s %s %s" % (basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 33_school_phone_number <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))
details = lines.map(lambda line : ("%s\t%s" % (line[32].encode('utf-8').strip(), col_details(line[32].encode('utf-8').strip()))))
details.saveAsTextFile("33_details.out")
sc.stop()
