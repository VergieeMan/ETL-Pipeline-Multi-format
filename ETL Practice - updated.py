# -*- coding: utf-8 -*-
"""
Created on Tue Dec  2 08:45:42 2025

@author: Vergi
"""

import glob
import os
import pandas as pd 
import xml.etree.ElementTree as ET
from datetime import datetime

#initiate
folder = "C:/Users/Vergi/.spyder-py3/ETL Files/ETL Practice"
headers = ["model","year_of_manufacture","price","fuel_type"]
target_file = "extracted_data.csv"
log_file = "log_file.txt"

#log Progress
def log_progress(message):
    timestamp_format = "%Y-%h-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp} | {message}\n")

#extract
def extract_from_csv(file_to_process):
    try:
        df = pd.read_csv(file_to_process)
        return df
    except Exception as e:
        print(f"Error processing CSV file {file_to_process} : {str(e)}")
        df = pd.DataFrame(headers)

def extract_from_json(file_to_process):
    try:
        df = pd.read_json(file_to_process, lines = True)
        return df
    except Exception as e:
        print(f"Error processing json file {file_to_process} : {str(e)}")
        df = pd.DataFrame(headers)

def extract_from_xml(file_to_process):
    rows = []
    try:
        tree = ET.parse(file_to_process)
        root = tree.getroot()
        for car in root:
            car_model = car.find("car_model").text
            year_of_manufacture = car.find("year_of_manufacture").text
            price = float(car.find("price").text)
            fuel = car.find("fuel").text
            rows.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price,"fuel":fuel})
        df = pd.DataFrame(rows)
        return df
    except Exception as e:
        print(f"Error processing XML file {file_to_process} : {str(e)}")
        df = pd.DataFrame(headers)
        
def extract():
    dataframe = []
    for csvfile in glob.glob(os.path.join(folder,"*.csv")):
        if os.path.basename(csvfile) != target_file:
            dataframe.append(extract_from_csv(csvfile))
    
    for jsonfile in glob.glob(os.path.join(folder, "*.json")):
        if os.path.basename(jsonfile) != target_file:
            dataframe.append(extract_from_json(jsonfile))
    
    for xmlfile in glob.glob(os.path.join(folder,"*.xml")):
        if os.path.basename(xmlfile) != target_file:
            dataframe.append(extract_from_xml(xmlfile))
    extracted_data = pd.concat(dataframe, ignore_index= True) if dataframe else pd.DataFrame(columns=headers)
    return extracted_data

#Transform
def transform(data):
    if data.empty:
        log_progress("WARNING, NO DATA TO BE TRANSFORMED")
        return data
    
    data["price"] = round(data.price,2)
    return data

#Load
def load_data(target_file, transformed_data):
    try:
        transformed_data.to_csv(target_file)
        print("Transformed Data")
        print(transformed_data)
    except Exception as e:
        print(f"Error: Failed to load data: {str(e)}")

#EXCUTION
log_progress("ETL JOB Started")
log_progress("Extraction phase started")
extracted_data = extract()
log_progress('Extraction phase ended')
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

log_progress("Transform phase ended")

log_progress("loading phase started")
load_data(target_file, transformed_data)

log_progress("Loading phase ended")
log_progress("ETL job finished")    
    
    