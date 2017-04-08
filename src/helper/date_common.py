from datetime import datetime

# Checks that the date is less than the other dates array
def date_less_than(value, others):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	for other in others:
		if other:
			dt2 = datetime.strptime(other, "%m/%d/%Y %I:%M:%S %p")
			if dt > dt2:
				return False
	return True

# Checks that the date is more than the other dates array
def date_more_than(value, others):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	for other in others:
		if other:
			dt2 = datetime.strptime(other, "%m/%d/%Y %I:%M:%S %p")
			if dt < dt2:
				return False
	return True

def date_in_valid_time_range(value):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	year = dt.year
	return True if (year >= 2009 and year <= 2017) else False