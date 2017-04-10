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
zip_lines = sc.textFile("/user/ac5901/zip.csv",1)
zips = dict(zip_lines.mapPartitions(lambda x: reader(x)).collect())

def check_zip(val):
        basetype = get_basetype(val)
	pattern=re.compile('^(\-?[\d+]{5}(-[\d+]{4})?)$')
        if basetype == 'INT':
                if val in zips.keys():
                        return 'VALID'
                else:
                        return 'INVALID'
        elif basetype == 'TEXT':
                if val is None or len(val.strip()) == 0 or val == 'Unspecified':
                        return 'NULL'
                elif pattern.match(val):
			if val.strip()[0:val.strip().find('-')] in zips.keys():
				return 'VALID'
			else:
	                        return 'INVALID'
		else:
			return 'INVALID'
        else:
                return 'INVALID'

def get_semantictype(val0, val1, basetype):
	pattern=re.compile('^(\-?[\d+]{5}(-[\d+]{4})?)$')
	if (basetype == 'INT' and len(str(val0)) == 5) or (pattern.match(val0)):
		return 'school_zip'
	elif basetype == 'TEXT' and (val0 is None or len(str(val0).strip())== 0 or str(val0) == 'Unspecified'):
		return 'school_zip'
	else:
		return 'None'

def col_details(val0, val1):
	basetype = get_basetype(val0)
	semantictype = get_semantictype(val0, val1, basetype)
	validity = check_zip(val0)
	return (val0, basetype, semantictype, validity)

if len(sys.argv) != 2:
	print("Usage: 37_school_zip <file>",  file=sys.stderr)
	exit(-1)
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))

details = lines.map(lambda line : (line[36].encode('utf-8').strip(), 1)) \
 			.reduceByKey(add) \
			.map(lambda x: col_details(x[0], x[1]))

details.map(lambda x: "%s\t%s %s %s" % (x[0], x[1], x[2], x[3])).saveAsTextFile("37_school_zip.out")

sc.stop()
