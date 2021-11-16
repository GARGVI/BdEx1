__author__ = 'Gemma'
_author__ = 'Gemma'

import pandas as pd
import os
from pathlib import Path


fn = os.path.join(os.path.abspath(os.path.dirname(__file__)), "Input\measures_hourly_test.csv")

lastMeteringPointId = None
amon_measures = {}
measurements = []
readers = []


# with pd.read_csv(fn, sep =';', encoding='utf8', iterator = True, names= ['meteringPointId','timestamp','value','period','unit','type' ]) as reader:
#     df = reader.get_chunk(10)
#     print(df)


for chunk in pd.read_csv(fn, sep =';', encoding='utf8', chunksize=100000, names= ['meteringPointId','timestamp','value','period','unit','type' ]):
    for row in chunk.itertuples():
        print(row.meteringPointId ,row.timestamp)