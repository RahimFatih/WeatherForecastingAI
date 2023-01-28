from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema
from PIL import Image, ImageDraw
from datetime import datetime
import numpy as np
import time
import os
import sys
import cv2
import pickle
spark=SparkSession.builder.master("spark://192.168.75.3:7077").appName("using_nn").getOrCreate()
sc = spark.sparkContext
pickle_in = open("model_trained.p", "rb")
model = pickle.load(pickle_in)
brodcasted_model = sc.broadcast(model)
def read_from_nn(dataset,date):
    results = [date]
    model = brodcasted_model.value
    for each in dataset:
        img = cv2.cvtColor(each, cv2.COLOR_BGR2GRAY)
        img = cv2.equalizeHist(img)
        img = img / 255
        img = img.reshape(1, 50, 50, 1)
        result = model.predict(img)
        rained = "Rained" if np.argmax(result) >0 else "Hasn't rained"
        results.append((rained,str(np.amax(result) * 100 ).split(".")[0] + "%"))
    return results



def procesing_images(rdf):
    date = rdf.image.origin
    date = date.split("/")[-1].split(".")[0]
    img = Image.frombytes(mode="RGB", data=bytes(rdf.image.data), size=[rdf.image.width,rdf.image.height])
    img.convert("L")
    box = (485,229,585,309)
    croped_country = img.crop(box)
    NW_v, SW_v, NE_v, SE_v = (-1,-1,-1,-1)
    NW = np.array(croped_country.crop((0,0,50,40)).resize((50,50)))[:, :, ::-1].copy() #NW
    SW = np.array(croped_country.crop((0,40,50,80)).resize((50,50)))[:, :, ::-1].copy() #SW
    NE = np.array(croped_country.crop((50,0,100,40)).resize((50,50)))[:, :, ::-1].copy() #NE
    SE = np.array(croped_country.crop((50,40,100,80)).resize((50,50)))[:, :, ::-1].copy() #SE
    
    end =read_from_nn((NW,SW,NE,SE),date)               
    return(end)


columns = ["Date", "NW", "SW", "NE", "SE"]
df_images = spark.read.format("image").load("./testing")
rdd = df_images.rdd
yas = rdd.map(lambda f:procesing_images(f)).toDF(columns)

yas.show(truncate=False)