import subprocess 
subprocess.check_call(['pip', 'install', 'pandas'])
subprocess.check_call(['pip', 'install', 'numpy'])
subprocess.check_call(['pip', 'install', 'google-cloud-storage'])
subprocess.check_call(['pip', 'install', 'pyspark'])
subprocess.check_call(['pip', 'install', 'sparkmagic'])

import numpy as np 
import pandas as pd 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
import io
import pandas as pd
from google.cloud import storage

# Créer un client pour accéder à Google Cloud Storage
client = storage.Client()

# Récupérer le fichier flights.csv à partir du bucket "bucket_zied"
bucket_name = 'bucket_zied'
blob_name = 'dataproc/flights.csv'
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)

# Télécharger le fichier CSV sur le disque
blob.download_to_filename('flights.csv')

# Lire le fichier CSV en utilisant pandas
data = pd.read_csv('flights.csv', header=0)

# Créer une session Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Convertir Dataframe pandas -> Dataframe Spark
csv = spark.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)

# Sélectionner les colonnes pour la classification 
data = csv.select("DayofMonth", "DayOfWeek", "Carrier", "OriginAirportID", "DestAirportID", "DepDelay", ((col("ArrDelay") > 15).cast("Int").alias("label")))

# Diviser le jeu de données en jeu d'entraînement et jeu de test
splits = data.randomSplit([0.7, 0.3])
train = splits[0]
test = splits[1].withColumnRenamed("label", "trueLabel")

# Compter le nombre de lignes dans chaque jeu de données
train_rows = train.count()
test_rows = test.count()
print("Training Rows:", train_rows, " Testing Rows:", test_rows)

"""
Création d'un pipeline pour entraîner un modèle de régression logistique

StringIndexer :  pour indexer une colonne de chaînes de caractères ("Carrier") en une colonne d'indices ("CarrierIdx").

VectorAssembler :  pour assembler un vecteur de colonnes en une seule colonne. Dans ce cas, les colonnes "CarrierIdx", "DayofMonth", "DayOfWeek", "OriginAirportID" et "DestAirportID" sont regroupées dans une seule colonne "catFeatures".

VectorIndexer :  pour indexer automatiquement les colonnes numériques dans le vecteur "catFeatures".

VectorAssembler :  pour assembler les colonnes "idxCatFeatures" et "numFeatures" en une seule colonne "features".

MinMaxScaler :  pour mettre à l'échelle les valeurs de la colonne "numFeatures" à l'aide de la méthode MinMax.

LogisticRegression :  pour entraîner un modèle de régression logistique à l'aide de la colonne "features" comme variables explicatives et la colonne "label" comme variable cible.

Pipeline :  pour regrouper toutes les étapes précédentes dans un pipeline, qui peut ensuite être utilisé pour entraîner et appliquer le modèle de régression logistique.
"""
strIdx = StringIndexer(inputCol = "Carrier", outputCol = "CarrierIdx")
catVect = VectorAssembler(inputCols = ["CarrierIdx", "DayofMonth", "DayOfWeek", "OriginAirportID", "DestAirportID"], outputCol="catFeatures")
catIdx = VectorIndexer(inputCol = catVect.getOutputCol(), outputCol = "idxCatFeatures")
numVect = VectorAssembler(inputCols = ["DepDelay"], outputCol="numFeatures")
minMax = MinMaxScaler(inputCol = numVect.getOutputCol(), outputCol="normFeatures")
featVect = VectorAssembler(inputCols=["idxCatFeatures", "normFeatures"], outputCol="features")
lr = LogisticRegression(labelCol="label",featuresCol="features",maxIter=10,regParam=0.3)
#dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
pipeline = Pipeline(stages=[strIdx, catVect, catIdx, numVect, minMax, featVect, lr])

# Entraîner le pipeline sur le training set 
piplineModel = pipeline.fit(train)

# On applique le modèle sur le test set
prediction = piplineModel.transform(test)

predicted = prediction.select("features", "prediction", "trueLabel")
predicted.show(100, truncate=False)

# Calcul des métriques
tp = float(predicted.filter("prediction == 1.0 AND truelabel == 1").count())
fp = float(predicted.filter("prediction == 1.0 AND truelabel == 0").count())
tn = float(predicted.filter("prediction == 0.0 AND truelabel == 0").count())
fn = float(predicted.filter("prediction == 0.0 AND truelabel == 1").count())

pr = tp / (tp + fp) # précision
re = tp / (tp + fn) # rappel

metrics = spark.createDataFrame([
 ("TP", tp),
 ("FP", fp),
 ("TN", tn),
 ("FN", fn),
 ("Precision", pr),
 ("Recall", re),
 ("F1", 2*pr*re/(re+pr))],["metric", "value"])
metrics.show()

# On évalue la perf du modèle en calculant l'aire sous la courbe ROC
evaluator = BinaryClassificationEvaluator(labelCol="trueLabel", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
aur = evaluator.evaluate(prediction)
print ("AUR = ", aur)
prediction.select("rawPrediction", "probability", "prediction", "trueLabel").show(100, truncate=False)

# On ajuste les hyperparametres
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.3, 0.1]).addGrid(lr.maxIter, [10, 5]).addGrid(lr.threshold, 
                                                                                            [0.4, 0.3]).build()
cv = CrossValidator(estimator=pipeline, evaluator=BinaryClassificationEvaluator(), estimatorParamMaps=paramGrid, 
                    numFolds=2)

# On entraîne le modèle avec les hyperparametres ajustés
model = cv.fit(train)

# On recalcule les métriques pour évaluer le modèle
newPrediction = model.transform(test)
newPredicted = prediction.select("features", "prediction", "trueLabel")
newPredicted.show()

# Recalculate confusion matrix
tp2 = float(newPrediction.filter("prediction == 1.0 AND truelabel == 1").count())
fp2 = float(newPrediction.filter("prediction == 1.0 AND truelabel == 0").count())
tn2 = float(newPrediction.filter("prediction == 0.0 AND truelabel == 0").count())
fn2 = float(newPrediction.filter("prediction == 0.0 AND truelabel == 1").count())
pr2 = tp2 / (tp2 + fp2)
re2 = tp2 / (tp2 + fn2)
metrics2 = spark.createDataFrame([
 ("TP", tp2),
 ("FP", fp2),
 ("TN", tn2),
 ("FN", fn2),
 ("Precision", pr2),
 ("Recall", re2),
 ("F1", 2*pr2*re2/(re2+pr2))],["metric", "value"])
metrics2.show()
# Recalculate the Area Under ROC
evaluator2 = BinaryClassificationEvaluator(labelCol="trueLabel", rawPredictionCol="prediction", metricName="areaUnderROC")
aur2 = evaluator.evaluate(prediction)
print( "AUR2 = ", aur2)