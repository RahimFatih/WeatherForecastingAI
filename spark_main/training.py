import os
import pickle
import cv2
import matplotlib.pyplot as mpl
import numpy as np
from keras.layers import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv2D, MaxPooling2D
from keras.models import Sequential
from keras.preprocessing.image import ImageDataGenerator
from keras.utils.np_utils import to_categorical
from sklearn.model_selection import train_test_split
from keras.optimizers import Adam


data_path = "./results"

weather_images_list = []
class_number = []
weather_images_type_list = os.listdir(data_path)
no_classes = len(weather_images_type_list)   

for img_type in range(no_classes):
    img_list = os.listdir(data_path + "/" + str(img_type))
    for img in img_list:
        weather_images_list.append(cv2.imread(data_path + "/" + str(img_type) + "/" + img))
        class_number.append(img_type)
        
# weather_images_list = np.array(weather_images_list)
class_number = np.array(class_number)

def img_standarization(img):
    img = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
    img = cv2.equalizeHist(img)
    img = img/255
    return img


weather_images_list = np.array(list(map(img_standarization,weather_images_list)))
weather_images_list = weather_images_list.reshape(weather_images_list.shape[0],weather_images_list.shape[1],weather_images_list.shape[2],1)

X_train, X_test, Y_train, Y_test = train_test_split(weather_images_list, class_number, test_size=0.2)

X_train, X_validation, Y_train, Y_validation = train_test_split(X_train, Y_train, test_size=0.2)

Y_train = to_categorical(Y_train, no_classes)
Y_validation = to_categorical(Y_validation, no_classes)
Y_test = to_categorical(Y_test, no_classes)

def cnn_model():
    model = Sequential()
    model.add(Flatten(input_shape = (50,50)))
    model.add(Dense(units=256, activation='relu'))
    model.add(Dense(units=128, activation='relu'))
    model.add(Dense(units=64, activation='relu'))
    model.add(Dense(units=2, activation='softmax'))
    model.summary()
    model.compile(Adam(lr=0.001), loss="categorical_crossentropy", metrics=["accuracy"])

    return model

model = cnn_model()

batch_size = 50
epoch_value = 80
step_per_epoch = 800

history = model.fit(
    x = X_train, 
    y = Y_train, 
    batch_size=batch_size,
    steps_per_epoch=step_per_epoch,
    epochs=epoch_value,
    validation_data=(X_validation, Y_validation))

mpl.figure(1)
mpl.plot(history.history["loss"])
mpl.legend(["training"])
mpl.title("Loss")
mpl.xlabel("epoch")

mpl.figure(2)
mpl.plot(history.history["accuracy"])
mpl.legend(["training"])
mpl.title("Accuracy")
mpl.xlabel("epoch")

mpl.show()

pickle_out = open("model_trained.p", "wb")
pickle.dump(model, pickle_out)
pickle_out.close()