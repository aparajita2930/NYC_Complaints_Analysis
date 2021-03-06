from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if len(sys.argv) != 2:
	print("Usage: comp_count <file>",  file=sys.stderr)
	exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1, use_unicode=False)
lines = lines.mapPartitions(lambda x: reader(x))    
    
count_locations = lines.map(lambda line : (line[7].encode('utf-8').strip(), 1)) \
                      .reduceByKey(add) \
		      .sortBy(lambda x: (-x[1], x[0])) \
                      .map(lambda x: "%s\t%s" % (x[0], x[1])) 
    
    
count_bour = lines.map(lambda line : (line[23].encode('utf-8').strip(), 1)) \
                      .reduceByKey(add) \
		      .sortBy(lambda x: (-x[1], x[0])) \
                      .map(lambda x: "%s\t%s" % (x[0], x[1]))
    
 
count_comp_by_location = lines.map(lambda line : ((line[5].encode('utf-8').strip(), line[7].encode('utf-8').strip()), 1)) \
                      .reduceByKey(add) \
		      .sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
                      .map(lambda x: "%s, %s\t%s" % (x[0][0], x[0][1], x[1]))
                                
                            
count_comp_by_city = lines.map(lambda line : ((line[5].encode('utf-8'), line[16].encode('utf-8')), 1)) \
                      .reduceByKey(add) \
		      .sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
                      .map(lambda x: "%s, %s\t%s" % (x[0][0], x[0][1], x[1]))
                              
count_bour.saveAsTextFile("bour_dist.out")                    
count_comp_by_city.saveAsTextFile("types_by_city.out")
count_comp_by_location.saveAsTextFile("types_by_location.out")
count_locations.saveAsTextFile("location_dist.out")

sc.stop()
