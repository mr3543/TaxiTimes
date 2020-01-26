# load all the 2018 taxi data, prep data for training and save to jsonl
# and parquet formats
import sys
import pandas as pd
from pyspark.sql            import SparkSession
from pyspark.sql.functions  import col,dayofweek,to_date,hour,month
from pyspark.sql.types      import StructField,StructType,StringType,IntegerType
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.holiday import Holiday


def main(base_path):
    spark = SparkSession.builder.appName('TaxiTimes').getOrCreate()

    # read in our dataset
    taxi_df = spark.read.format('csv') \
                   .options(header='true',treatEmptyValuesAsNull='true',
                            inferSchema='true') \
                   .load('{}/data/yellow_tripdata_2018_01.csv'.format(base_path))

    # read in the zone keys
    zones   = spark.read.format('csv') \
                   .options(header='true',treatEmptyValuesAsNull='true',
                            inferSchema='true') \
                   .load('{}/data/taxi_zone_lookup.csv'.format(base_path))

    # drop cols that aren't available at prediction time
    cols_to_drop = ['VendorID','RatecodeID','store_and_fwd_flag',
                    'payment_type','fare_amount','mta_tax','tip_amount',
                    'tolls_amount','improvement_surcharge','total_amount',
                    'extra']

    taxi_df  = taxi_df.drop(*cols_to_drop)

    # create target column
    duration = (col('tpep_dropoff_datetime').cast('long') - col('tpep_pickup_datetime').cast('long'))/60
    taxi_df  = taxi_df.withColumn('duration',duration)

    # filter outliers
    taxi_df  = taxi_df.where('duration < 120')

    pu_join_expr = col('PULocationID') == col('LocationID')
    do_join_expr = col('DOLocationID') == col('LocationID')

    # join zones on pickup location 
    zones = zones.select('LocationID','Borough')
    taxi_df  = taxi_df.join(zones,pu_join_expr,'inner') \
                      .drop('LocationID') \
                      .withColumnRenamed('Borough','PUBorough')

    # join zones on dropoff location
    taxi_df  = taxi_df.join(zones,do_join_expr,'inner') \
                      .drop('LocationID') \
                      .withColumnRenamed('Borough','DOBorough')

    # make change borough variable - true if taxi ride changes boroughs
    change_borough_expr = (col('PUBorough') != col('DOBorough')).cast('integer')
    taxi_df  = taxi_df.withColumn('change_borough',change_borough_expr) \
                      .drop('PULocationID','DOLocationID')

    # extract time/date features from the pickup time variable - want to know the hour
    # of the day, month & day of week
    taxi_df  = taxi_df.withColumn('day_of_week',dayofweek(col('tpep_pickup_datetime'))) \
                      .withColumn('pickup_time',hour(col('tpep_pickup_datetime'))) \
                      .withColumn('month',month(col('tpep_pickup_datetime'))) \
                      .withColumn('pickup_date',to_date('tpep_pickup_datetime','yyyy-mm-dd')) \
                      .drop('tpep_pickup_datetime','tpep_dropoff_datetime')

    # create calendar to determine if travel date is a holiday
    cal = USFederalHolidayCalendar()

    # add NYE and Xmas eve which are busy travel days but not included in built-in calendar
    # for further feature building we could lookup NYC parade dates and include them here
    nye = Holiday('NYE',month=12,day=31)
    xmas_eve = Holiday('xmas_eve',month=12,day=24)
    cal.rules += [nye,xmas_eve]
    holidays = cal.holidays(start='2018-01-01',end='2018-12-31').to_pydatetime()

    # make holiday df which we will join with our training data 
    holidays = [str(h.date()) for h in holidays]
    holiday_schema = StructType([StructField('holiday_date',StringType(),True),
                                 StructField('is_holiday',IntegerType(),True)])

    holiday_df = spark.createDataFrame([*zip(holidays,[1]*len(holidays))],holiday_schema)

    # join on date key
    holiday_join_expr = col('pickup_date') == col('holiday_date')
    taxi_df = taxi_df.join(holiday_df,col('pickup_date') == col('holiday_date'),'left') \
                     .drop('holiday_date','pickup_date') \
                     .fillna({'is_holiday':0})

    # save formatted data to disk 
    taxi_df.write.format('parquet').mode('overwrite')\
           .save('{}/data/2018_taxi_data.parquet'.format(base_path))

if __name__ == '__main__':
    main(sys.argv[1])

