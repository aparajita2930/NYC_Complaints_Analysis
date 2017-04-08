from datetime import datetime

# Checks that the date is less than the other date
def date_less_than(value, other):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	dt2 = datetime.strptime(other, "%m/%d/%Y %I:%M:%S %p")
	return True if dt < dt2 else False

# Checks that the date is more than the other date
def date_more_than(value, other):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	dt2 = datetime.strptime(other, "%m/%d/%Y %I:%M:%S %p")
	return True if dt > dt2 else False

def date_in_valid_time_range(value):
	dt = datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
	year = dt.year
	return True if (year >= 2009 and year <= 2017) else False