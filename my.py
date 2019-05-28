import pandas
import decimal

#python my.py

#SELECT cab_type,
#       count(*)
#FROM trips
#GROUP BY cab_type;
def q1():
	df = pandas.read_csv('trips_xaa.csv',compression=None,header=None,names=['trip_id','vendor_id','pickup_datetime','dropoff_datetime','store_and_fwd_flag',
    'rate_code_id','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude','passenger_count','trip_distance','fare_amount','extra',
    'mta_tax','tip_amount','tolls_amount','ehail_fee','improvement_surcharge','total_amount','payment_type','trip_type','pickup','dropoff','cab_type',
    'precipitation','snow_depth','snowfall','max_temperature','min_temperature','average_wind_speed','pickup_nyct2010_gid','pickup_ctlabel','pickup_borocode',
    'pickup_boroname','pickup_ct2010','pickup_boroct2010','pickup_cdeligibil','pickup_ntacode','pickup_ntaname','pickup_puma','dropoff_nyct2010_gid',
    'dropoff_ctlabel','dropoff_borocode','dropoff_boroname','dropoff_ct2010','dropoff_boroct2010','dropoff_cdeligibil','dropoff_ntacode','dropoff_ntaname','dropoff_puma'],)

#SELECT cab_type,
#       count(*)
#FROM trips
#GROUP BY cab_type;
	#print(df.groupby('cab_type')['cab_type'].count())

#SELECT passenger_count,
#       avg(total_amount)
#FROM trips
#GROUP BY passenger_count;
	#print(df.groupby('passenger_count',as_index=False).mean()[['passenger_count','total_amount']])

#SELECT passenger_count,
#       EXTRACT(year from pickup_datetime) as year,
#       count(*)
#FROM trips
#GROUP BY passenger_count,
#         year;
#	transformed = df[['passenger_count','pickup_datetime']].transform({'passenger_count':lambda x: x,'pickup_datetime':lambda x:  pandas.DatetimeIndex(x).year})
#	print(transformed.groupby(['passenger_count','pickup_datetime'])[['passenger_count','pickup_datetime']].count()['passenger_count'] )

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
	transformed = df[['passenger_count','pickup_datetime','trip_distance']].transform({'passenger_count':lambda x:x, 'pickup_datetime':lambda x:pandas.DatetimeIndex(x).year, 'trip_distance': lambda x:decimal.Decimal(x).quantize(0, decimal.ROUND_HALF_UP)})
	print(transformed.groupby(['passenger_count','pickup_datetime','trip_distance'])[['passenger_count','pickup_datetime','trip_distance']].count()['passenger_count'])
	#print(transformed.size().reset_index().sort_values(by=['pickup_datetime',0],ascending=[True,False]))

q1()
