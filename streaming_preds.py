import sys,os,re
import json
import datetime,iso8601
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel,VectorAssembler
from pyspark.ml.regression import RandomForestRegressor,RandomForestRegressionModel 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col,from_json

##################################################################################
#
# usage: spark-submit --master [master] --packages [spark-sql-kafka],[spark-monogo] \
#              streaming-preds.py [basepath] [refresh period] [kafka broker port] \
#              [kafka topic]
#
################################################################################## 


def main(basepath,period,kafka_port,topic):

    APP_NAME = 'streaming_preds.py'
    brokers = 'localhost:' + str(kafka_port) 
    spark = SparkSession.builder.master('local') \
                        .appName(APP_NAME) \
                        .config('spark.mongodb.output.uri','mongodb://127.0.0.1/TaxiTimes.from_kafka') \
                        .getOrCreate()

    # load string indexer models
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

    # create json schema
    schema   = StructType([StructField('passenger_count',IntegerType(),True),
                       StructField('trip_distance',DoubleType(),True),
                       StructField('PUBorough',StringType(),True),
                       StructField('DOBorough',StringType(),True),
                       StructField('change_borough',IntegerType(),True),
                       StructField('day_of_week',IntegerType(),True),
                       StructField('month',IntegerType(),True),
                       StructField('pickup_time',IntegerType(),True),
                       StructField('is_holiday',IntegerType(),True)])
    
    # define the foreach batch function which we will apply to each micro-batch
    # this takes the stream, makes the prediciton and saves to mongoDB
    def foreach_batch_function(batch_df,batch_id):
        if len(batch_df.head(1)) == 0:
            return
        # need to convert the kafka value field in json binary to its own 
        # dataframe so we can feed it to the model
        batch_df = batch_df.select(col('timestamp'),col('value'))
        batch_df = batch_df.withColumn('from_kafka',from_json(col('value').cast('string'),schema)) \
                           .withColumn('passenger_count',col('from_kafka.passenger_count')) \
                           .withColumn('trip_distance',col('from_kafka.trip_distance')) \
                           .withColumn('PUBorough',col('from_kafka.PUBorough')) \
                           .withColumn('DOBorough',col('from_kafka.DOBorough')) \
                           .withColumn('change_borough',col('from_kafka.change_borough')) \
                           .withColumn('day_of_week',col('from_kafka.day_of_week')) \
                           .withColumn('month',col('from_kafka.month')) \
                           .withColumn('pickup_time',col('from_kafka.pickup_time')) \
                           .withColumn('is_holiday',col('from_kafka.is_holiday')) \
                           .drop('from_kafka','value')

        # apply indexers and vectorizer and make prediction
        for col_name in str_names:
            str_model = string_index_models[col_name]
            batch_df = str_model.transform(batch_df)
        batch_df = va.transform(batch_df)
        to_drop = ['PUBorough_index','DOBorough_index']
        batch_df = rf.transform(batch_df)
        batch_df = batch_df.drop(*to_drop,'feature_vec')

        # write to mongo db
        batch_df.write.format('mongo').mode('append').save()  
        
        return 

    #################################################################
    # start streaming from kafka source
    #################################################################
    df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', topic) \
            .load()

    df_query = df.writeStream.foreachBatch(foreach_batch_function).start()
    df_query.awaitTermination()

if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])

