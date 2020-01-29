import sys,os,re
import json
import datetime,iso8601
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel,VectorAssembler
from pyspark.ml.regression import RandomForestRegressor,RandomForestRegressionModel 
from pyspark.sql.types import StructType, StructField


def main(iso_date,basepath):
   
    APP_NAME = 'make_predictions.py'
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    
    # load the string indexer models
    string_index_models = {}
    str_names = ['PUBorough','DOBorough']
    for col_name in str_names:
        model_path = '{}/models/StringIndexer_{}.bin'.format(basepath,col_name)
        string_index_models[col_name] = StringIndexerModel.load(model_path)

    # load the vector assembler
    va_path = '{}/models/VectorAssembler.bin'.format(basepath)
    va      = VectorAssembler.load(va_path)

    # load the model
    rf_path = '{}/models/RandomForestRegressor.bin'.format(basepath)
    rf      = RandomForestRegressionModel.load(rf_path)

    # read the requets from disk
    request_path = '{}/data/prediction_tasks_daily/{}_.parquet'.format(
      basepath,
      iso_date)

    requests = spark.read.format('parquet').load(request_path)
    
    # do string indexing
    for col_name in str_names:
        str_model = string_index_models[col_name]
        requests  = str_model.transform(requests)

    # transform to vector and make predictions
    requests = va.transform(requests)
    to_drop  = ['PUBorough_index','DOBorough_index']
    requests = requests.drop(*to_drop)

    predictions = rf.transform(requests)
    predictions = predictions.drop('feature_vec')
    
    # save predictions to disk
    out_path = '{}/data/prediction_results_daily/{}_.parquet'.format(
      basepath,
      iso_date)
    
    predictions.write.format('parquet') \
               .mode('overwrite') \
               .save(out_path)
    
if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2])


