from __future__ import print_function

import sys, os, re
from operator import add
from pyspark import SparkContext
from csv import reader

def get_label(value, basetype,semantictype):
	if basetype == "TEXT":
		if not value:
			return "NULL"
		else:
			if semantictype == "GEO_CODE":
				value = value.replace("(","").replace(")","")
				loc = value.split(",")
				if check_valid_geo(loc[0],loc[1]):
					return "VALID"
				else:
					return "INVALID"
			else:
				return "INVALID"
	else:
		return "INVALID"

def create_semantics(value, basetype):
	if basetype == "TEXT":
		res = re.search("^\((\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)\)$", value)
		if res:
			return "GEO_CODE"
	return "TEXT"

def create_labels(value):
	basetype = get_basetype(value)
	semantictype = create_semantics(value,basetype)
	label = get_label(value, basetype,semantictype)	
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: 52_location.py <file>",  file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	sc.addFile("src/helper/assign_basetype.py")
	sc.addFile("src/helper/geo_common.py")
	from assign_basetype import *
	from geo_common import *
	lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
	lines = lines.mapPartitions(lambda x: reader(x)) 
	details_loc = lines.map(lambda line : ("%s\t%s" % (line[51].encode('utf-8').strip(), create_labels(line[51].encode('utf-8').strip()))))
	details_loc.saveAsTextFile("52_details.out")
	sc.stop()