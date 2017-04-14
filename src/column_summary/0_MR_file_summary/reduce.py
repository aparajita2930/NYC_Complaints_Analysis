#!/usr/bin/env python
import sys, string

#Create data structures to hold the current row/column values (if needed; your code goes here)
keyTotal = 0
currentkey = None

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

	#Remove leading and trailing whitespace
	line = line.strip()

	#Get key/value 
	key, value = line.split('\t',1)

	#If we are still on the same key...
	if key==currentkey:

		#Process key/value pair (your code goes here)
		keyTotal += int(value)

	#Otherwise, if this is a new key...
	else:
		#If this is a new key and not the first key we've seen
		if currentkey:
			#compute/output result to STDOUT (your code goes here)
			print '%s\t%s' % (currentkey, keyTotal)
	
		currentkey = key
		#Process input for new key (your code goes here)
		keyTotal = 0
		keyTotal += int(value)

#Compute/output result for the last key (your code goes here)
print '%s\t%s' % (currentkey, keyTotal)
