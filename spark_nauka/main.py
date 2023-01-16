from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema
from PIL import Image, ImageDraw
import os
import sys
spark=SparkSession.builder.master("local[").appName("test").getOrCreate()


def procesing_images(rdf):
    img = Image.frombytes(mode="RGB", data=bytes(rdf.image.data), size=[rdf.image.width,rdf.image.height])
    img.convert("L")
    box = (485,229,585,309)
    croped_country = img.crop(box)
    NW = croped_country.crop((0,0,50,40)) #NW
    SW = croped_country.crop((0,40,50,80)) #SW
    NE = croped_country.crop((50,0,100,40)) #NE
    SE = croped_country.crop((50,40,100,80)) #SE
    
    return(rdf.image.origin,NW,0,SW,1,NE,2,SE,3)
def print_rdf(rdf):
    print(rdf)
    
df_images = spark.read.format("image").load("./Images")
rdd = df_images.rdd
yas = rdd.map(lambda f:procesing_images(f))


print(type(yas))
