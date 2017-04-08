from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper/assign_basetype import assign_basetype

reload(sys)
sys.setdefaultencoding('utf-8')

def get_label(value, basetype):
	if basetype == "str":
		return "NULL"
	elif basetype == "DATETIME":
		dt = datetime.datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")


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
    summary_count = lines.map(lambda line : (line[7].encode('utf-8').strip(), 1))
    summary_count.saveAsTextFile("2_created_date_summary.out")
    sc.stop()