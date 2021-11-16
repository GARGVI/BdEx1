# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 11:41:18 2014

This function push the UTE data from CVS files.

python push_UTE.py 


@author: daniel
"""


from copy import deepcopy
import sys
import os
import requests
import urllib3
import json
import pandas as pd

from multiprocessing import Process, Pool
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#from requests.packages.urllib3 import disable_warnings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


##URLs
base_url_measures = 'https://api.empowering.cimne.com/v1/amon_measures'
base_url_log = 'https://api.empowering.cimne.com/authn/login'


headers = {
    'X-CompanyId': '1234509876',
    'content-type': 'application/json'
}

# When login CompanyId must be excluded from headers
headers_login = {
    'content-type': 'application/json'
}
# User and Password
payload = {
    "username": "test@test",
    "password":"test1234"
}

##Certificates
cert_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Cert\pclient0-api-cimne.crt")
key_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Cert\client0-api-cimne.key")
cert = (cert_file_path, key_file_path)

# Obtaining the token from the login, this information is needed to
# perform the request to the new server
def login():
    res = requests.post(base_url_log, headers=headers_login, data=json.dumps(payload), verify=False)
    cookie = res.json()['token']
    cookie = {
            'iPlanetDirectoryPro': cookie
    }
    return cookie



# Loading Data
# amon_measures = {
#                   "measurements":
#                   [
#                     {
#                       "timestamp": "2014-10-11T16:37:05Z",
#                       "type": "electricityConsumption",
#                       "value": 7.0
#                     },
#                     {
#                       "timestamp": "2014-10-11T16:37:05Z",
#                       "type": "electricityKiloVoltAmpHours",
#                       "value": 11.0
#                     }
#                   ],
#                   "meteringPointId": "c1810810-0381-012d-25a8-0017f2cd3574",
#                   "readings":
#                   [
#                     {"type": "electricityConsumption", "period": "INSTANT", "unit": "kWh"},
#                     {"type": "electricityKiloVoltAmpHours", "period": "INSTANT", "unit": "kVArh"}
#                   ],
#                   "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574"
#                 }
#
# cookie = login()
#
# res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures), cert=cert, verify=False)
# print (res.json())

cookie = login()

fn = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Input\measures_hourly_test.csv")

for chunk in pd.read_csv(fn, sep =';', encoding='utf8', chunksize=10000, names= ['meteringPointId','timestamp','value','period','unit','type' ]):
    for row in chunk.itertuples():
        if lastMeteringPointId and lastMeteringPointId != row.meteringPointId:
            amon_measures = {"measurements": measurements,
                             "readers": readers,
                             "meteringPointId": lastMeteringPointId,
                             "deviceId": lastMeteringPointId}

            res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures), cert=cert, verify=False)
            # inicialitzo
            measurements = []
            readers = []

        measurements.append({"timestamp":row.timestamp,
                             "type": row.type,
                             "value":row.value})

        readers.append({"type": row.type,
                        "period":row.period,
                        "unit":row.unit})

        lastMeteringPointId = row.meteringPointId

amon_measures = {"measurements": measurements,
                 "readers": readers,
                 "meteringPointId": lastMeteringPointId,
                 "deviceId": lastMeteringPointId}

res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures), cert=cert, verify=False)

