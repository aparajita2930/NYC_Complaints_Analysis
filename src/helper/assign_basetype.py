from datetime import datetime
import re
from collections import OrderedDict

def raise_(ex):
    raise ex

basetypes = OrderedDict()
basetypes[int] = int
basetypes[float] = float
basetypes['date'] = (lambda x: datetime.strptime(x, "%m/%d/%Y"))
basetypes['datetime'] = (lambda x: datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p"))
basetypes['geo_code'] = (lambda x: 1 if ( re.search("^\((\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)\)$", x)) else raise_(ValueError('Error')))

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
