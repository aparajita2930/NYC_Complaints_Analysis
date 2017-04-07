from datetime import datetime
import re
from collections import OrderedDict

basetypes = OrderedDict({int: int, float: float, 'date': (lambda dt: datetime.strptime(dt, "%m/%d/%Y")), 'datetime': (lambda dt_time: datetime.strptime(dt_time, "%m/%d/%Y %I:%M:%S %p")), 'geo_code': (lambda geo_code: re.match("^\((\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)\)$", geo_code))})

def get_basetype(val):
	for basetype, try_type in basetypes.items():
		try:
			try_type(val)
			return basetype	
		except ValueError:
			continue
	return str


values = ['10.3', '12/31/2010', '12/31/2010 03:10:09 AM', '15', 'hello', "(40.65662129596871, -73.95806621423951)" ]
for val in values:
	print val, get_basetype(val)
