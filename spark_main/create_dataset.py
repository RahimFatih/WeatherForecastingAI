from pyspark.sql import SparkSession, functions
from pyspark.sql.window import Window
from pyspark.ml.image import ImageSchema
from PIL import Image, ImageDraw
from datetime import datetime
import time
import os
import sys
spark=SparkSession.builder.master("spark://192.168.75.3:7077").appName("create_dataset").getOrCreate()

csv_grouping = spark.read.option("header", "true").csv("./dataGroupedByQuarters.csv").collect()
df_images = spark.read.format("image").load("./Images").rdd
sc = spark.sparkContext

brodcasted_model = sc.broadcast(csv_grouping)
def create_rdf(rdf):
    date = 0
    NW_v, SW_v, NE_v, SE_v = [-1,-1,-1,-1]
    for csv_data in brodcasted_model.value:
        date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(csv_data.Data) - 2).strftime('%Y_%m_%d')
        if date in rdf.image.origin:
            NW_v = 0 if float(csv_data.NW) < 2 else 1
            NE_v = 0 if float(csv_data.NE) < 2 else 1
            SW_v = 0 if float(csv_data.SW) < 2 else 1
            SE_v = 0 if float(csv_data.SE) < 2 else 1
            break
    return [rdf.image, date,NW_v,NE_v,SW_v,SE_v]        

def procesing_images(rdf):
    img = Image.frombytes(mode="RGB", data=bytes(rdf.Image.data), size=[rdf.Image.width,rdf.Image.height])
    img.convert("L")
    box = (485,229,585,309)
    croped_country = img.crop(box)
    NW = croped_country.crop((0,0,50,40)).resize((50,50)) #NW
    SW = croped_country.crop((0,40,50,80)).resize((50,50)) #SW
    NE = croped_country.crop((50,0,100,40)).resize((50,50)) #NE
    SE = croped_country.crop((50,40,100,80)).resize((50,50)) #SE
                    
    
    
    return[NW, rdf.NW,SW, rdf.SW ,NE, rdf.NE ,SE ,rdf.SE]

columns =["Image", "Date", "NW", "NE", "SW", "SE"]

grouped_img = df_images.map(lambda f:create_rdf(f)).toDF(columns).rdd

final_img = grouped_img.map(lambda i:procesing_images(i))


# idx = 1
# for each in final_img:
#     for i in range(0,8,2):
#         if each[i+1] != -1:
#             each[i].save("./results_test/" + str(each[i+1]) + "/" + str(idx) + ".jpg")
#             idx += 1
            
