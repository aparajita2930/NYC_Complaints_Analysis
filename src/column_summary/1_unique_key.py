from __future__ import print_function

import sys
import os
from operator import add
from pyspark import SparkContext
from csv import reader
#import imp

#sys.path.append(os.getcwd() + '../helper/')
#import helper.assign_basetype as assign_basetype

#assign_basetype = imp.load_source('get_basetype','../helper/assign_basetype.py')
#assign_basetype.get_basetype('15')

from datetime import datetime
import re
from collections import OrderedDict

def raise_(ex):
    raise ex

basetypes = OrderedDict()
basetypes['INT'] = int
basetypes['DECIMAL'] = float
basetypes['DATE'] = (lambda x: datetime.strptime(x, "%m/%d/%Y"))
basetypes['DATETIME'] = (lambda x: datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p"))
#basetypes['GEO_CODE'] = (lambda x: 1 if ( re.search("^\((\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)\)$", x)) else raise_(ValueError('Error')))

def get_basetype(val):
  for basetype, try_type in basetypes.items():
    try:
      try_type(val)
      return basetype
    except ValueError:
      continue
  return 'TEXT'

'''def split_tuple(full_tuple, l):
    indv = [iter(full_tuple)] * l
    return zip(*indv)'''

def get_validity(val0, val1):
	if len(val0.strip()) == 0:
		return (val0, 'NULL')
	elif val1 == 1:
		return (val0, 'VALID')
	#elif val1 > 1:
	#	return split_tuple((val0, 'INVALID')*val1, 2)
	else:
		return (val0, 'INVALID')

def col_details(val0, val1):
	return (val0, get_basetype(val0), get_validity(val0, val1)[1])

if len(sys.argv) != 2:
	print("Usage: 1_unique_key <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))

details_unique_key = lines.map(lambda line : (line[0].encode('utf-8').strip(), 1)) \
 			.reduceByKey(add) \
			.map(lambda x: col_details(x[0], x[1]))

details_unique_key.map(lambda x: "%s\t%s %s" % (x[0], x[1], x[2])).saveAsTextFile("1_unique_key.out")
