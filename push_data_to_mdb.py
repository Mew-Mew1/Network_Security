import os 
import sys
import json
from dotenv import load_dotenv
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)
'''
- The script loads environment variables using `python-dotenv`.  
- It retrieves the `MONGO_DB_URL` from the `.env` file.  
- Running `python push_data.env` fails because that’s not a Python script.  
- Running `python push_data_to_mdb.py` works and prints the MongoDB URI.  
- This confirms the environment variable is set correctly and accessible in Python.
'''

import certifi
ca = certifi.where()

'''
- The `certifi` package provides Mozilla’s trusted root certificates.  
- It ensures secure HTTPS connections by verifying SSL/TLS certificates.  
- In Python, libraries like `requests` and `pymongo` use it to validate server certificates.  
- Without `certifi`, you might face SSL errors when connecting to secure services.  
- Printing `certifi.where()` shows the path to the bundled certificate file used for validation.
'''

import pandas as pd
import numpy as np
import pymongo

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

class NetworkDataExtract:

    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        

    def csv_to_json_converter(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop = True, inplace = True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def insert_data_mongo(self, records, collection, database):
        try:
            self.database = database
            self.records = records
            self.collection = collection

            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)

            return(len(self.records))##It returns the length to confirm how many records were inserted into MongoDB.
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
if __name__ == "__main__":
    FILE_PATH = r"C:\Projects\Project2\Network_Data\phisingData.csv"#Ensures the code inside runs only when you launch this file, preventing accidental execution during imports.
    DATA_BASE = "NetworkDB"
    COLLECTION = "Networkdata"
    network_obj = NetworkDataExtract()
    records = network_obj.csv_to_json_converter(file_path = FILE_PATH)
    print(records)
    no_of_records = network_obj.insert_data_mongo(records, DATA_BASE, COLLECTION)
    print(no_of_records)
