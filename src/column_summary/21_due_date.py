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

def get_label(value, basetype, date_range):
	if basetype == "TEXT":
		if not value:
			return "NULL"
		else:
			return "INVALID"
	elif (basetype == "DATETIME" and date_in_valid_time_range(value) and date_more_than(value,date_range)):
		return "VALID"
	else:
		return "INVALID"


def create_labels(value, created_date, closed_date, resolution_due_date):
	basetype = get_basetype(value)
	semantictype = None
	label = get_label(value, basetype,[created_date])
	return "%s %s %s" % (basetype, semantictype, label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: 21_due_date.py <file>",  file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
    #lines = lines.map(lambda x: x.split(","))
    lines = lines.mapPartitions(lambda x: reader(x)) 
    details_due_date = lines.map(lambda line : ("%s\t%s" % (line[21].encode('utf-8').strip(), create_labels(line[21].encode('utf-8').strip(),line[2].encode('utf-8').strip(),line[3].encode('utf-8').strip(),line[22].encode('utf-8').strip()))))
    details_due_date.saveAsTextFile("21_due_date_details.out")
    sc.stop()