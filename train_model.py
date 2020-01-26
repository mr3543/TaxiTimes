from pyspark.sql.types     import StringType, IntegerType, FloatType, BooleanType, StructType, StructField
from pyspark.ml.feature    import StringIndexer, VectorAssembler
from pyspark.sql           import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import sys

def main(base_path):
    spark = SparkSession.builder.appName('TaxiTimes').getOrCreate()

    train_df  = spark.read.parquet('{}/data/2018_taxi_data.parquet'.format(base_path))
    str_cols  = ['PUBorough','DOBorough']
    
    # index our two string columns which denote the pick up and dropoff boroughs
    for col in str_cols:
        string_indexer = StringIndexer(inputCol=col,outputCol= col + '_index')
        string_model   = string_indexer.fit(train_df)
        train_df       = string_model.transform(train_df) 
        train_df       = train_df.drop(col)
        indx_path      = '{}/models/StringIndexer_{}.bin'.format(base_path,col)
        string_model.write().overwrite().save(indx_path)

    # create feature vector
    input_cols = ['PUBorough_index','DOBorough_index','passenger_count',
                  'trip_distance','change_borough','day_of_week','pickup_time',
                  'month','is_holiday']
    va = VectorAssembler(inputCols=input_cols,outputCol='feature_vec')
    train_df = va.transform(train_df)
    train_df = train_df.drop(*input_cols)
    va_path  = '{}/models/VectorAssembler.bin'.format(base_path)
    va.write().overwrite().save(va_path)

    # train random forest model and save model to disk
    rf = RandomForestRegressor(featuresCol='feature_vec',labelCol='duration',maxDepth=25,
                               numTrees=10,maxBins=32,minInstancesPerNode=5)
    rf_model = rf.fit(train_df)
    rf_path  = '{}/models/RandomForestRegressor.bin'.format(base_path)
    rf_model.write().overwrite().save(rf_path)

if __name__ == '__main__':
    main(sys.argv[1])








