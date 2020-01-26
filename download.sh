#!/bin/bash

mkdir data

# download 2018 taxi data and save to data folder

for i in $(seq -w 1 12)
do
    wget "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-$i.csv" \
    --no-check-certificate -O "$PROJECT_HOME/data/yellow_tripdata_2018_$i.csv"

done

wget "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" \
--no-check-certificate -O "$PROJECT_HOME/data/taxi_zone_lookup.csv"
