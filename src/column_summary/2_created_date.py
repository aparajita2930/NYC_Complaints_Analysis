from __future__ import print_function

import sys, os
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'helper'))
from assign_basetype import *
from date_common import *
#from assign_basetype import assign_basetype
reload(sys)
sys.setdefaultencoding('utf-8')

def get_label(value, basetype):
	if basetype == "TEXT":
		if not value:
			return "NULL"
		else:
			return "INVALID"
	elif (basetype == "DATETIME" and date_in_valid_time_range(value)):
		return "VALID"
	else:
		return "INVALID"


def create_labels(value):
	basetype = get_basetype(value)
	semantictype = None
	label = get_label(value, basetype)
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: 2_created_date.py <file>",  file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
    #lines = lines.map(lambda x: x.split(","))
    lines = lines.mapPartitions(lambda x: reader(x)) 
    details_created_date = lines.map(lambda line : ("%s\t%s" % (line[2].encode('utf-8').strip(), create_labels(line[2].encode('utf-8').strip()))))
    details_created_date.saveAsTextFile("2_created_date_summary.out")
    sc.stop()