
'''
Source : https://www.mapdevelopers.com/geocode_bounding_box.php
North Latitude: 40.917577
South Latitude: 40.477399
East Longitude: -73.700272
West Longitude: -74.259090
'''
max_lat = 40.917577
min_lat = 40.477399
max_lng = -73.700272
min_lng = -74.259090

def check_valid_lat(val):
	lat = float(val)
	return True if lat <= max_lat and lat >= min_lat else False

def check_valid_lng(val):
	lng = float(val)
	return True if lng <= max_lng and lng >= min_lng else False

def check_valid_geo(lat,lng):
	return (check_valid_lat(lat) and check_valid_lng(lng))