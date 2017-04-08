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

