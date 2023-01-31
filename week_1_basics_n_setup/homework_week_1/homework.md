
 select count(*)  from green_taxi_schema where date(lpep_pickup_datetime)='2019-01-15' and  date(lpep_dropoff_datetime)='2019-01-15

select trip_distance , date(lpep_pickup_datetime),date(lpep_dropoff_datetime) from green_taxi_schema order by trip_distance desc limit 

select passenger_count,count(*) from green_taxi_schema where date(lpep_pickup_datetime)='2019-01-01' and passenger_count in ('2','3') group by passenger_count 


select  "DOLocationID" , "Zone" from taxi_lookup join  green_taxi_schema  on "LocationID" ="DOLocationID" 
where tip_amount in
 (select max(tip_amount)  from taxi_lookup join  green_taxi_schema  on "LocationID" ="PULocationID" and "Zone"='Astoria')