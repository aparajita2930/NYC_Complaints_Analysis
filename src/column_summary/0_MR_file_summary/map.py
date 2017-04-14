#!/usr/bin/env python
# map function for task 1

import sys, os, csv
import string

# input comes from STDIN (stream data that goes to the program)
for line in csv.reader(sys.stdin, delimiter='\t'):
    
	#Remove leading and trailing whitespace
	#line = line.strip()

	#Split line into array of entry data
	value = line[1]
	key = value.split(' ')
	print '%s\t%s' % ('1_' + key[0],1)
	print '%s\t%s' % ('2_' + key[1],1)
	print '%s\t%s' % ('3_' + key[2],1)    

