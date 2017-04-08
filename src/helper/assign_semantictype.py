sementicType = OrderedDict()
sementicType['GEO_CODE'] = (lambda x: 1 if ( re.search("^\((\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)\)$", x)) else raise_(ValueError('Error')))

#def get_sementicType(val,columnName,basetype):