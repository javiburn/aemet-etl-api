import requests
import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime

def load_idemas(api_key):
    endpoint = 'https://opendata.aemet.es/opendata/api/observacion/convencional/todas/?api_key=' + api_key
    response = requests.get(endpoint)
    data = response.json()
    if response.status_code == 200:
        # Obtener la URL de los datos
        data_url = data['datos']
        response_data = requests.get(data_url)
        climatology_data = response_data.json()
        print(climatology_data)
        f = open("./docs/idema_list.txt", "w")
        idemas = [item["idema"] for item in climatology_data]
        idemas_str = '\n'.join(idemas)
        f.write(idemas_str)
        f.close()
