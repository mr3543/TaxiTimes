from flask import Flask,request
from pymongo import MongoClient
from bson import json_util
from kafka import KafkaProducer, TopicPartition
import json
import datetime,iso8601
import uuid

app = Flask(__name__)
client = MongoClient()

broker_port = 9092
producer = KafkaProducer(bootstrap_servers=['localhost:{}'.format(broker_port)],
                         api_version=(2,4))
TOPIC    = 'taxi_time_prediction_request'
TOPIC    = 'test2'

feature_types = {
        'passenger_count':int,
        'trip_distance':float,
        'PUBorough':str,
        'DOBorough':str,
        'change_borough':int,
        'day_of_week':int,
        'month':int,
        'pickup_time':int,
        'is_holiday':int}

@app.route('/predict_streaming',methods=['POST'])
def make_prediction_request_streaming():
    input_values = {}
    for feat_name,feat_type in feature_types.items():
        input_values[feat_name] = request.form.get(feat_name,type=feat_type)
     
    message_bytes = json.dumps(input_values).encode()
    producer.send(TOPIC,message_bytes)

    response = {'status':'OK'}
    return json.dumps(response)

@app.route('/predict',methods=['POST'])
def make_prediction_record():
    
    input_values = {}
    for feat_name,feat_type in feature_types.items():
        input_values[feat_name] = request.form.get(feat_name,type=feat_type)

    # add a timestamp
    timestamp = datetime.datetime.now()
    input_values['timestamp'] = timestamp
    # set the derived values
    client.TaxiTimes.prediction_tasks.insert_one(input_values)

    return json_util.dumps(input_values)
    

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
