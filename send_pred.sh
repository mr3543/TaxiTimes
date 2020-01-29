#!/bin/bash

curl -XPOST 'http://localhost:5000/predict_streaming' \
-F 'passenger_count=1' \
-F 'trip_distance=5.0' \
-F 'PUBorough=Manhattan' \
-F 'DOBorough=Queens' \
-F 'change_borough=1' \
-F 'day_of_week=2' \
-F 'month=1' \
-F 'pickup_time=15' \
-F 'is_holiday=0'

