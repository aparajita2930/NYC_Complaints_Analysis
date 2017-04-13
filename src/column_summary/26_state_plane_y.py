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
			return "INVALID"
	if basetype == "INT":
		return "VALID"
	else:
		return "INVALID"

def create_semantics(value, basetype):
	if basetype == "INT" and int(value) > 0:
		return "STATE_PLANE_CORD"
	return "TEXT"

def create_labels(value):
	basetype = get_basetype(value)
	semantictype = create_semantics(value,basetype)
	label = get_label(value, basetype,semantictype)	
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: 26_state_plane_y <file>",  file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	sc.addFile("src/helper/assign_basetype.py")
	sc.addFile("src/helper/geo_common.py")
	from assign_basetype import *
	from geo_common import *
	lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
	lines = lines.mapPartitions(lambda x: reader(x)) 
	details_loc = lines.map(lambda line : ("%s\t%s" % (line[25].encode('utf-8').strip(), create_labels(line[25].replace(',','').encode('utf-8').strip()))))
	details_loc.saveAsTextFile("26_details.out")
	sc.stop()