import yaml
from flask import Flask, request
import os
from datetime import datetime, timezone, timedelta
from docs import idema_listing
from dotenv import load_dotenv
from model.connector import Connector
from model.source import AEMETSource
from model.sink import BigQuerySink
from model.config import ConnectorConfig
from google.cloud import bigquery
from config.create_table import create_table_with_schema
import pandas as pd
import threading
import signal
import sys
import json

app = Flask(__name__)

def load_idemas(file_path: str) -> list:
    with open(file_path, 'r') as file:
        idemas = file.read().splitlines()
    return idemas

def table_has_schema(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        if table.schema:
            return True
        else:
            return False
    except Exception as e:
        if "Not found" in str(e):
            print(f"Table '{table_id}' not found in dataset '{dataset_id}'.")
        else:
            print(f"Error checking table schema: {e}")
        return False

def incremental_load_task():
    load_dotenv()
    api_key = os.getenv("API_KEY")
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    idemas_path = 'docs/idema_list.txt'
    max_delta_time_str = config['max_delta_time']
    delta_values = max_delta_time_str.split()
    try:
        max_delta_time = timedelta(days=int(delta_values[0]))
    except (ValueError, IndexError) as e:
        print(f"Error converting delta value: {e}")
        max_delta_time = timedelta(days=1)  # Default value or another fallback mechanism
    connector_config = ConnectorConfig(max_delta_time=max_delta_time)
    start_date_str = config['start_date'].replace("UTC", "")
    start_date = datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)
    project_id = config['bigquery_project_id']
    dataset_id = config['bigquery_dataset_id']
    table_id = config['bigquery_table_id']
    source = AEMETSource(api_key=api_key)
    sink = BigQuerySink(project_id, dataset_id, table_id)
    connector = Connector(config=connector_config, source=source, sink=sink, start_date=start_date, max_delta_time=max_delta_time)
    
    try:
        file_size = os.path.getsize(idemas_path)
        if file_size < 18000:
            idema_listing.load_idemas(api_key)
    except FileNotFoundError as e:
        print("File NOT found")
    
    idemas = load_idemas(config['idema_file'])
    client = bigquery.Client(project=project_id)
    if not table_has_schema(client, dataset_id, table_id):
        create_table_with_schema(project_id, dataset_id, table_id)
    
    for object_id in idemas:
        # Load JSON data
        with open('docs/pd_frame.json') as json_file:
            data = json.load(json_file)

        # Create DataFrame
        df = pd.DataFrame(data)
        print(df.columns)  # Print the columns to debug

        # Specify data types for problematic columns, if they exist
        dtype_spec = {
            'sol': 'float64',
            'presmax': 'float64',
            'presmin': 'float64',
            'horahrmax': 'float64',
            'horahrmin': 'float64',
            'tmed': 'float64',
            'prec': 'float64',
            'tmin': 'float64',
            'tmax': 'float64',
            'dir': 'float64',
            'velmedia': 'float64',
            'racha': 'float64',
            'hrmedia': 'float64',
            'hrmax': 'float64',
            'hrmin': 'float64'
        }

        for column, dtype in dtype_spec.items():
            if column in df.columns:
                df[column] = df[column].astype(dtype)
            else:
                print(f"Column '{column}' not found in data")

        if 'hrmedia' in df.columns:
            hrmedia_data = df['hrmedia']
            # Additional processing here
        else:
            print("Column 'hrmedia' not found")

        connector.incremental_load(object_id)


# Manejador de señal SIGTERM
def sigterm_handler(signal, frame):
    print('SIGTERM received. Stopping incremental load.')
    sys.exit(0)

signal.signal(signal.SIGTERM, sigterm_handler)

@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/trigger', methods=['POST'])
def trigger_incremental_load():
    load_thread = threading.Thread(target=incremental_load_task)
    load_thread.start()
    return 'Incremental load started', 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
