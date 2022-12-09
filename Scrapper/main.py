import selenium
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from threading import Thread
from selenium.webdriver.chrome.options import Options
import requests
from datetime import datetime, timedelta
from calendar import monthrange
from selenium.webdriver.support.ui import Select




def downloadImage(url,name):
    img_data = requests.get(url).content
    with open('./Images/'+name+".png", 'wb') as handler:
        handler.write(img_data)

def task(year,month):
    start_date = datetime(year, month, 1,0)
    end_date = start_date + timedelta(days=(monthrange(year,month)[1]-1))
    delta = end_date - start_date

    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless=True
    #prefs = {"profile.managed_default_content_settings.images": 2}
    #chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome("./chromedriver.exe",chrome_options=chrome_options)
    driver.get("https://www.sat24.com/history.aspx")
    
    
    drop_down_months = Select(driver.find_element(By.ID,'ctl00_maincontent_dropdownListMonth'))
    drop_down_years = Select(driver.find_element(By.ID,'ctl00_maincontent_dropdownListYear'))

    infrared_checkbox = driver.find_element(By.ID,"ctl00_maincontent_checkBoxInfrared")

    infrared_checkbox.click()

    drop_down_years.select_by_value(str(year))
    drop_down_months.select_by_value(str(month))

    for i in range(delta.days + 1):
        drop_down_days = Select(driver.find_element(By.ID,'ctl00_maincontent_dropdownListDay'))
        drop_down_days.select_by_value(str(i+1))
        for j in range(24):
            drop_down_hours = Select(driver.find_element(By.ID,'ctl00_maincontent_dropdownListHour'))
            drop_down_hours.select_by_value(str(j))
            retrive_button = driver.find_element(By.ID,"ctl00_maincontent_buttonRetrieve")
            retrive_button.click()
            print(driver.find_element(By.ID,"ctl00_maincontent_imageSat").get_attribute("src"))
            downloadImage(driver.find_element(By.ID,"ctl00_maincontent_imageSat").get_attribute("src"),str(year)+"_"+str(month)+"_"+str(str(i+1))+"_"+str(str(j)))

    #time.sleep(5)

    driver.close()

for rok in range(2015,2022):
    for i in range(12):
        thread = Thread(target=task,args=(rok,i))
        thread.start()
    print('Waiting for the thread...')
    thread.join()
