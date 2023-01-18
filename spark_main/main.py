from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema
from PIL import Image, ImageDraw
from datetime import datetime
import time
import os
import sys
spark=SparkSession.builder.master("spark://192.168.100.7:7077").appName("test").getOrCreate()


def procesing_images(rdf):
    img = Image.frombytes(mode="RGB", data=bytes(rdf.image.data), size=[rdf.image.width,rdf.image.height])
    img.convert("L")
    box = (485,229,585,309)
    croped_country = img.crop(box)
    NW_v, SW_v, NE_v, SE_v = (-1,-1,-1,-1)
    NW = croped_country.crop((0,0,50,40)).resize((50,50)) #NW
    SW = croped_country.crop((0,40,50,80)).resize((50,50)) #SW
    NE = croped_country.crop((50,0,100,40)).resize((50,50)) #NE
    SE = croped_country.crop((50,40,100,80)).resize((50,50)) #SE
                    
    
    
    return(rdf.image.origin,NW,SW,NE,SE)

    
csv_grouping = spark.read.option("header", "true").csv("./dataGroupedByQuarters.csv")
df_images = spark.read.format("image").load("./Images")
rdd = df_images.rdd
yas = rdd.map(lambda f:procesing_images(f))

i = 0
j = 0
k = 1
for podzielona_polska in yas.collect():
    print(k)
    for wiersz_z_csv in csv_grouping.collect():
        date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(wiersz_z_csv.Data) - 2).timetuple()
        date = str(date[0]) + '_' + str(date[1]) + '_' + str(date[2])
        if date in podzielona_polska[0]:
            if wiersz_z_csv.Cwiartka == "NW":
                if float(wiersz_z_csv.SrednieOpady) > 3:
                    podzielona_polska[1].save("./results/1/" + str(i) + ".jpg")
                    i += 1
                else:
                    podzielona_polska[1].save("./results/0/" + str(j) + ".jpg")
                    j += 1
            elif wiersz_z_csv.Cwiartka == "SW":
                if float(wiersz_z_csv.SrednieOpady) > 3:
                    podzielona_polska[2].save("./results/1/" + str(i) + ".jpg")
                    i += 1
                else:
                    podzielona_polska[2].save("./results/0/" + str(j) + ".jpg")
                    j += 1
            elif wiersz_z_csv.Cwiartka == "NE":
                if float(wiersz_z_csv.SrednieOpady) > 3:
                    podzielona_polska[3].save("./results/1/" + str(i) + ".jpg")
                    i += 1 
                else:
                    podzielona_polska[3].save("./results/0/" + str(j) + ".jpg")
                    j += 1
            elif wiersz_z_csv.Cwiartka == "SE":
                if float(wiersz_z_csv.SrednieOpady) > 3:
                    podzielona_polska[4].save("./results/1/" + str(i) + ".jpg")
                    i += 1
                else:
                    podzielona_polska[4].save("./results/0/" + str(j) + ".jpg")
                    j += 1
    k += 1
