



from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def get_semantictype(basetype, label):
	if label == 'NULL' or label == 'VALID':
		return 'STATUS'
	else:
		return 'None'

def get_label(val, basetype):
	if basetype == 'TEXT':
		if val is None or len(val.strip()) == 0 or val.upper() in ['UNASSIGNED', 'UNSPECIFIED']:
			return 'NULL'
		elif val.upper() in ['ASSIGNED', 'CANCELLED','CLOSED','CLOSED - BY PHONE','CLOSED - EMAIL SENT','CLOSED - IN-PERSON','CLOSED - INSUFFICIENT INFO','CLOSED - LETTER SENT','CLOSED - NO RESPONSE NEEDED','CLOSED - OTHER','CLOSED - TESTING','DRAFT','EMAIL SENT','IN PROGRESS','IN PROGRESS - NEEDS APPROVAL','OPEN','PENDING','STARTED','TO BE REROUTED','UNABLE TO RESPOND' ]:
			return 'VALID'
		else:
			return 'INVALID'
	else:
		return 'INVALID'


def create_labels(value):
	basetype = get_basetype(value)
	label = get_label(value, basetype)
	semantictype = get_semantictype(basetype, label)
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: 20_status <file>",  file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	sc.addFile("src/helper/assign_basetype.py")
	from assign_basetype import *
	lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
	lines = lines.mapPartitions(lambda x: reader(x)) 
	details = lines.map(lambda line : ("%s\t%s" % (line[19].encode('utf-8').strip(), create_labels(line[19].encode('utf-8').strip()))))
	details.saveAsTextFile("20_details.out")
	sc.stop()
