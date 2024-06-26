import requests
import pandas as pd
from datetime import datetime
from typing import Protocol

class Source(Protocol):
    def extract_object(
        self,
        object_id: str,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> pd.DataFrame:
        ...

class AEMETSource:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos"

    def extract_object(self, object_id: str, start_datetime: datetime, end_datetime: datetime) -> pd.DataFrame:
        headers = {
            'cache-control': 'no-cache',
            'api_key': self.api_key,
        }
        params = {
            'start': start_datetime.strftime('%Y-%m-%dT%H:%M:%SUTC'),
            'end': end_datetime.strftime('%Y-%m-%dT%H:%M:%SUTC'),
            'idema': object_id
        }
        endpoint = f"{self.base_url}/fechaini/{params['start']}/fechafin/{params['end']}/estacion/{params['idema']}/?api_key={self.api_key}"
        
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        
        try:
            data_url = response.json().get('datos', None)
            if not data_url:
                raise ValueError("No data URL found in response.")
        except Exception as e:
            print(f"Error extracting data URL: {e}")
            return pd.DataFrame({'A': []})
        
        response_data = requests.get(data_url)
        response_data.raise_for_status()
        
        data = response_data.json()
        
        # Create DataFrame and convert the 'fecha' column to datetime type
        df = pd.DataFrame(data)
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # List of all possible columns based on the JSON structure
        required_columns = [
            'fecha', 'indicativo', 'nombre', 'provincia', 'altitud', 'tmed', 'prec', 'tmin', 'horatmin',
            'tmax', 'horatmax', 'dir', 'velmedia', 'racha', 'horaracha', 'sol', 'presmax', 'horapresmax',
            'presmin', 'horapresmin', 'hrmedia', 'hrmax', 'horahrmax', 'hrmin', 'horahrmin'
        ]

        # Add missing columns with NaN values
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA

        # Convert specified columns to float where applicable
        float_columns = [
            'altitud', 'tmed', 'prec', 'tmin', 'tmax', 'dir', 'velmedia', 'racha', 'hrmedia', 'hrmax', 'hrmin'
        ]

        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
