from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema
from PIL import Image, ImageDraw
from datetime import datetime
import os
import sys
spark=SparkSession.builder.master("spark://192.168.100.7:7077").appName("test").getOrCreate()


def procesing_images(rdf,group):
    img = Image.frombytes(mode="RGB", data=bytes(rdf.image.data), size=[rdf.image.width,rdf.image.height])
    img.convert("L")
    box = (485,229,585,309)
    croped_country = img.crop(box)
    NW_v, SW_v, NE_v, SE_v = (-1,-1,-1,-1)
    NW = croped_country.crop((0,0,50,40)).resize((50,50)) #NW
    SW = croped_country.crop((0,40,50,80)).resize((50,50)) #SW
    NE = croped_country.crop((50,0,100,40)).resize((50,50)) #NE
    SE = croped_country.crop((50,40,100,80)).resize((50,50)) #SE
    for each in csv_grouping.collect():
        date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(each.Data) - 2).timetuple()
        date = str(date[0]) + '_' + str(date[1]) + '_' + str(date[2])
        if date in rdf.image.origin:
            if each.Cwiartka == "NW":
                if int(each.SrednieOpady) > 5:
                    NW_v = 1 
                else:
                    NW_v = 0
            elif each.Cwiartka == "SW":
                if int(each.SrednieOpady) > 5:
                    SW_v = 1 
                else:
                    SW_v = 0
            elif each.Cwiartka == "NE":
                if int(each.SrednieOpady) > 5:
                    NE_v = 1 
                else:
                    SW_v = 0
            elif each.Cwiartka == "SE":
                if int(each.SrednieOpady) > 5:
                    SE_v = 1 
                else:
                    SW_v = 0
            if NW_v != -1 and SW_v != -1 and NE_v != -1 and SE_v != -1:
                break
                    
    
    
    return(rdf.image.origin,NW,NW_v,SW,SW_v,NE,NE_v,SE,SE_v)
def print_rdf(rdf):
    print(rdf)
    
csv_grouping = spark.read.option("header", "true").csv("./dataGroupedByQuarters.csv")
df_images = spark.read.format("image").load("./Images")
rdd = df_images.rdd
yas = rdd.map(lambda f:procesing_images(f,csv_grouping))


for each in yas.collect():
    print(each)
