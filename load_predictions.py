import sys,os,re
import json
import datetime,iso8601

from bson import json_util
from pyspark.sql import SparkSession

def main(iso_date,base_path):
    
    APP_NAME = 'load_prediction_results.py'
    spark    = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark = SparkSession.builder.master('local') \
                        .appName(APP_NAME) \
                        .config('spark.mongodb.output.uri','mongodb://127.0.0.1/TaxiTimes.prediction_results') \
                        .getOrCreate()

    # read in the predicts and write to mongo db
    input_path = '{}/data/prediction_results_daily/{}_.parquet'.format(
      base_path,
      iso_date)

    preds = spark.read.format('parquet').load(input_path)
    preds.write.format('mongo').mode('append').save()


if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2])
