import sys,os,re
import json
import datetime,iso8601

from bson import json_util
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, to_date, date_add, lit


def main(iso_date,base_path):

    APP_NAME = 'fetch_prediction_requests.py'
    spark = SparkSession.builder \
                        .appName(APP_NAME) \
                        .config('spark.mongodb.input.uri','mongodb://127.0.0.1/TaxiTimes.prediction_tasks') \
                        .getOrCreate()

    today_dt = to_date(lit(iso_date),'yyyy-mm-dd') 

    # schema for prediction requests stored in mongodb
    schema   = StructType([StructField('passenger_count',IntegerType(),True),
                           StructField('trip_distance',DoubleType(),True),
                           StructField('PUBorough',StringType(),True),
                           StructField('DOBorough',StringType(),True),
                           StructField('change_borough',IntegerType(),True),
                           StructField('day_of_week',IntegerType(),True),
                           StructField('month',IntegerType(),True),
                           StructField('pickup_time',IntegerType(),True),
                           StructField('is_holiday',IntegerType(),True),
                           StructField('timestamp',TimestampType(),True)])

    # read mongodb requets
    tasks = spark.read.format('mongo') \
                 .schema(schema) \
                 .load()

    # filter for today's date
    tasks = tasks.where(to_date(col('timestamp')) == today_dt)
    
    # write to parquet
    today_output_path = '{}/data/prediction_tasks_daily/{}_.parquet'.format(
       base_path,
       iso_date)
   
    tasks.write.format('parquet').mode('overwrite')\
         .save(today_output_path)

if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2])


