__author__ = 'Gemma'

import pandas as pd
import os
from pathlib import Path
import json


#fn = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Input\measures_hourly_test.csv")
fn = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Input\prova.csv")

lastMeteringPointId = None
amon_measures = {}
measurements = []
readers = []


# with pd.read_csv(fn, sep =';', encoding='utf8', iterator = True, names= ['meteringPointId','timestamp','value','period','unit','type' ]) as reader:
#     df = reader.get_chunk(10)
#     print(df)

f = open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "Output\provaOutput.txt"), "w")

for chunk in pd.read_csv(fn, sep =';', encoding='utf8', chunksize=2, names= ['meteringPointId','timestamp','value','period','unit','type' ]):
    for row in chunk.itertuples():
        if lastMeteringPointId and lastMeteringPointId != row.meteringPointId:
            amon_measures = {"measurements": measurements,
                             "readers": readers,
                             "meteringPointId": lastMeteringPointId,
                             "deviceId": lastMeteringPointId}



#         # fer POST
#         # res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures), cert=cert, verify=False)
            f.write(json.dumps(amon_measures))
            f.write('\n')
#           # inicialitzo
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

f.write(json.dumps(amon_measures))
f.write('\n')
       #print(amon_measures)
f.close





# for row in  chunk:
#     (meteringPointId, timestamp, value, period, unit, type) = row
#     if lastMeteringPointId and lastMeteringPointId != meteringPointId:
#         amon_measures = {"measurements": measurements,
#                          "readers": readers,
#                          "meteringPointId": lastMeteringPointId,
#                          "deviceId": lastMeteringPointId}
#
#         # fer POST
#         # res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures), cert=cert, verify=False)
#
#         # inicialitzo
#         measurements = []
#         readers = []
#
#
#
#         measurements.append({"timestamp":timestamp,
#                              "type": type,
#                              "value":value})
#
#         readers.append({"type": type,
#                         "period":period,
#                         "unit":unit})
#
#         lastMeteringPointId = meteringPointId
#
#
#
#         lastMeteringPointId = lastMeteringPointId
#         print(amon_measures)
