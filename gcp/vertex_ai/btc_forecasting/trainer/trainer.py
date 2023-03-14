import os
import numpy as np 
import pandas as pd 
from matplotlib import pyplot as plt
import sklearn
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from google.cloud import storage

from keras.models import load_model

                            ### GET DATA ###

# Récupération de l'input à partir du bucket
client = storage.Client()

# Récupérer le fichier flights.csv à partir du bucket "bucket_zied"
bucket_name = 'bucket_zied_eu'
blob_name = 'vertex_ai/btc_data.csv'
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)

# Télécharger le fichier CSV sur le disque
blob.download_to_filename('btc_data.csv')

# Lire le fichier CSV en utilisant pandas
df = pd.read_csv('btc_data.csv', header=0)

df['date'] = pd.to_datetime(df['Timestamp'],unit='s').dt.date
group = df.groupby('date')
Real_Price = group['Weighted_Price'].mean()

# split data
prediction_days = 30
df_train= Real_Price[:len(Real_Price)-prediction_days]
df_test= Real_Price[len(Real_Price)-prediction_days:]

                            ### PREPROCESS DATA ###

training_set = df_train.values
training_set = np.reshape(training_set, (len(training_set), 1))
sc = MinMaxScaler()
training_set = sc.fit_transform(training_set)
X_train = training_set[0:len(training_set)-1]
y_train = training_set[1:len(training_set)]
X_train = np.reshape(X_train, (len(X_train), 1, 1))

from keras.models import Sequential
from keras.layers import LSTM, Dense

                            ### BUILD THE RNN ###

# Initializing the RNN
regressor = Sequential()

# Adding the input layers
regressor.add(LSTM(units=4, activation='sigmoid', input_shape=(None, 1)))

# Adding the output layers
regressor.add(Dense(units=1))

# Compiling the RNN
regressor.compile(optimizer='Adam', loss='mean_squared_error')

# Fitting the RNN to the training set
regressor.fit(X_train, y_train, batch_size=5, epochs=100)


                            ### PREDICTIONS ###

test_set = df_test.values
inputs = np.reshape(test_set, (len(test_set), 1))
inputs = sc.transform(inputs)
inputs = np.reshape(inputs, (len(inputs), 1, 1))
predicted_BTC_price = regressor.predict(inputs)
predicted_BTC_price = sc.inverse_transform(predicted_BTC_price)


                            ### SAVE MODEL & SCALER ###


from os import path, getenv, makedirs, listdir

BUCKET_PATH = getenv('AIP_MODEL_DIR')
# BUCKET_PATH = '/output'
MODEL_PATH = path.join(BUCKET_PATH, 'btc-model')
SCALER_PATH = path.join(BUCKET_PATH, 'scaler.bin')
SCALER_PATH = SCALER_PATH.replace('gs://bucket_zied_eu/', '')
print("BUCKET_PATH: ", BUCKET_PATH)

# Create output directory
makedirs(BUCKET_PATH, exist_ok=True)
makedirs(MODEL_PATH, exist_ok=True)
# Save model
regressor.save(MODEL_PATH)

# Save scaler
import joblib
file_sc = joblib.dump(sc, 'std_scaler.bin', compress=True)
#store on bucket
from google.cloud import storage
# The ID of your GCS bucket
client=storage.Client("glossy-precinct-371813")
bucket=client.get_bucket('bucket_zied_eu')
blob=bucket.blob(SCALER_PATH)
blob.upload_from_filename('std_scaler.bin')

print("BUCKET_PATH: ", BUCKET_PATH)
print("Done.")






MODEL_PATH='bucket_zied_eu/output/model'
SCALER_PATH='bucket_zied_eu/output/scaler'


regressor.save(MODEL_PATH)


import joblib
file_sc = joblib.dump(sc, 'scaler.bin', compress=True)
blob=bucket.blob(SCALER_PATH)
blob.upload_from_filename('scaler.bin')
