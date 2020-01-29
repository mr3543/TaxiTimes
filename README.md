# TaxiTimes

This repo contains a functioning example of a scalable prediction pipeline for batch processing and real time predictions. We use mongoDB, pyspark, kafka and airflow. 

To download the training data run `download.sh`. Our dataset comes from the NYC TLC and contains trip data for yellow taxi cab rides during the year 2018. Our app allows users to submit information about their ride and get back a prediction for the duration of the trip. 

Apache-airflow is used to coordinate the data processing and handling of batch predictions. The `airflow/airflow_setup.py` script creates two DAGS. One for pre-processing the training data and training the model, and another for loading prediction requets from mongoDB and making predictions. 

`process_data.py` contains data pre-processing and feature engineering. 
`train_model.py` trains a random forest model using spark MLLib. 
`fetch_prediction_requests.py` loads prediction requests from mongoDB and saves these requests to disk
`make_predictions.py` loads prediction requets from disk and uses our random forest model to predict the trip's duration.
`load_predictions.py` takes our predictions and pushs them to the database
`web/taxi_flask.py` a simple flask web-sever which takes prediction requests and saves them to our database
`send_pred.sh` simple curl script to send prediction requets to web-server
`streaming_preds.py` allows for streaming predictions of requets. Here were receive kafka messages, make the predictions,
and write the results to mongodb.
