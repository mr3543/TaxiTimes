import os,sys,re
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime,timedelta
import iso8601

basepath   = '/home/mmr/TaxiTimes'
master     = 'local'
start_date = '2020-01-22'
email      = 'mr3543@columbia.edu'
   
default_args = {'owner':'airflow',
                'depends_on_past':True,
                'email':[email],
                'email_on_failure':True,
                'email_on_retry':False,
                'start_date':iso8601.parse_date(start_date),
                'retries':3,
                'retry_delay':timedelta(minutes=5)}

training_dag = DAG('TaxiTimes_model_training',
               default_args=default_args,
               schedule_interval=timedelta(1))

pyspark_training_command    = """spark-submit --master {{ params.master }} \
                                 {{ params.filename }} {{ params.basepath }}"""

pyspark_batch_mongo_command = """spark-submit --master {{ params.master }} \
                                 --packages {{ params.mongo_spark }} \
                                 {{ params.filename }} {{ ds }} \
                                 {{ params.basepath }}"""

pyspark_batch_command       = """spark-submit --master {{ params.master }} \
                                 {{ params.filename }} {{ ds }} \
                                 {{ params.basepath }}"""

process_data_operator = BashOperator(task_id = 'process_data', 
                                     bash_command = pyspark_training_command,
                                     params = {'master':master,
                                               'basepath':basepath,
                                               'filename':basepath+'/process_data.py'},
                                    dag=training_dag)
train_model_operator = BashOperator(task_id = 'train_model',
                                    bash_command = pyspark_training_command,
                                    params = {'master':master,
                                              'basepath':basepath,
                                              'filename':basepath + '/train_model.py'},
                                    dag = training_dag)

process_data_operator >> train_model_operator

batch_prediction_dag = DAG('TaxiTimes_batch_prediction',
                           default_args = default_args)

fetch_predictions_operator = BashOperator(task_id = 'fetch_predictions',
                                          bash_command = pyspark_batch_mongo_command,
                                          params = {'master':master,
                                                    'basepath':basepath,
                                                    'filename':basepath+'/fetch_prediction_requests.py',
                                                    'mongo_spark': \
                                                    'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1'},
                                          dag = batch_prediction_dag)

make_predictions_operator = BashOperator(task_id = 'make_predictions',
                                         bash_command = pyspark_batch_command,
                                         params = {'master':master,
                                                   'basepath':basepath,
                                                   'filename':basepath+'/make_predictions.py'},
                                        dag = batch_prediction_dag)

load_predictions_operator = BashOperator(task_id = 'load_predictions',
                                         bash_command = pyspark_batch_mongo_command,
                                         params = {'master':master,
                                                   'basepath':basepath,
                                                   'filename':basepath+'/load_predictions.py',
                                                   'mongo_spark': \
                                                   'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1'},
                                        dag = batch_prediction_dag)

fetch_predictions_operator >> make_predictions_operator >> load_predictions_operator



