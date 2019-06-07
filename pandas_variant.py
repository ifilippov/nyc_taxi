import pandas
import decimal
import sys
import datetime

#python pandas_variant.py

def taxi1(df):
	#SELECT cab_type,
	#       count(*)
	#FROM trips
	#GROUP BY cab_type;
	print("\nNAME: Taxi number 1");
	df['count'] = ""
	return df[['cab_type', 'count']].groupby('cab_type').count().reset_index()

def taxi2(df):
	#SELECT passenger_count,
	#       avg(total_amount)
	#FROM trips
	#GROUP BY passenger_count;
	print("\nNAME: Taxi number 2");
	return df.groupby('passenger_count',as_index=False)['total_amount'].mean()

def taxi3(df):
	#SELECT passenger_count,
	#       EXTRACT(year from pickup_datetime) as year,
	#       count(*)
	#FROM trips
	#GROUP BY passenger_count,
	#         year;
	print("\nNAME: Taxi number 3");
	df = df[['passenger_count','pickup_datetime']].transform({'passenger_count':lambda x: x,'pickup_datetime':lambda x:  pandas.DatetimeIndex(x).year})
	df['count'] = ""
	return df.groupby(['passenger_count','pickup_datetime']).count().reset_index()

def taxi4(df):
	#SELECT passenger_count,
	#       EXTRACT(year from pickup_datetime) as year,
	#       round(trip_distance) distance,
	#       count(*) trips
	#FROM trips
	#GROUP BY passenger_count,
	#         year,
	#         distance
	#ORDER BY year,
	#         trips desc;
	print("\nNAME: Taxi number 4");
	df = df[['passenger_count','pickup_datetime','trip_distance']].transform({'passenger_count':lambda x:x, 'pickup_datetime':lambda x:pandas.DatetimeIndex(x).year, 'trip_distance': lambda x:decimal.Decimal(x).quantize(0, decimal.ROUND_HALF_UP)})
	df['trips'] = ""
	return df.groupby(['passenger_count','pickup_datetime','trip_distance']).count().reset_index().sort_values(by=['pickup_datetime','trips'],ascending=[True, False])

def measurement(a, table):
	b = datetime.datetime.now()
	print "DONE!", (b - a).seconds, "seconds /", (b - a).seconds * 1000 + (b - a).microseconds / 1000, "milliseconds"
	if table is not None:
		print "\n***RESULT***"
		print table.to_string(index = False, header = False)
	return datetime.datetime.now()

filename = 'trips_xaa.csv'
if len(sys.argv) > 1:
	filename = sys.argv[1]

print("\nThread number: unknown");

print("\nTASK: loading CSV file");
a = datetime.datetime.now()
df = pandas.read_csv(filename, compression=None, header=None, names=['trip_id','vendor_id','pickup_datetime','dropoff_datetime','store_and_fwd_flag',
    'rate_code_id','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude','passenger_count','trip_distance','fare_amount','extra',
    'mta_tax','tip_amount','tolls_amount','ehail_fee','improvement_surcharge','total_amount','payment_type','trip_type','pickup','dropoff','cab_type',
    'precipitation','snow_depth','snowfall','max_temperature','min_temperature','average_wind_speed','pickup_nyct2010_gid','pickup_ctlabel','pickup_borocode',
    'pickup_boroname','pickup_ct2010','pickup_boroct2010','pickup_cdeligibil','pickup_ntacode','pickup_ntaname','pickup_puma','dropoff_nyct2010_gid',
    'dropoff_ctlabel','dropoff_borocode','dropoff_boroname','dropoff_ct2010','dropoff_boroct2010','dropoff_cdeligibil','dropoff_ntacode','dropoff_ntaname','dropoff_puma'],)
a = measurement(a, None)
a = measurement(a, taxi1(df))
a = measurement(a, taxi2(df))
a = measurement(a, taxi3(df))
measurement(a, taxi4(df))
