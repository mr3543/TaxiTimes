from flask import Flask,request
from pymongo import MongoClient
from bson import json_util
import json
import datetime,iso8601

app = Flask(__name__)
client = MongoClient()

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
