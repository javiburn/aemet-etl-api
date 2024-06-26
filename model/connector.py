import logging
import google.cloud.logging
from google.cloud.logging import handlers
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from .sink import BigQuerySink, Sink
from .source import AEMETSource
import pandas as pd
import numpy as np

class Connector:
    def __init__(self, config, source: AEMETSource, sink: BigQuerySink, start_date: datetime, max_delta_time: timedelta):
        # Initialize the client
        client = google.cloud.logging.Client()
        handler = handlers.CloudLoggingHandler(client)
        handler.setLevel(logging.INFO)
        
        # Set up Python's logging to use this handler
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        
        self.config = config
        self.source = source
        self.sink = sink
        self.start_date = start_date
        self.max_delta_time = max_delta_time

    def incremental_load(self, object_id: str):
        last_update_datetime = self.sink.get_last_update_datetime(object_id)
        start_datetime = max(last_update_datetime, self.start_date)
        end_datetime = datetime.now(timezone.utc)  # Use timezone-aware datetime
        self.extract_and_load_object(object_id, start_datetime, end_datetime)

    def extract_and_load_object(self, object_id: str, start_datetime: datetime, end_datetime: datetime) -> None:
        self.logger.info(f"Extracting and loading object: {object_id} from {start_datetime} to {end_datetime}")
        t0 = start_datetime
        while t0 < end_datetime:
            print(f"Extracting and loading object: {object_id} from {t0} to {end_datetime}")
            t1 = min(t0 + self.config.max_delta_time, end_datetime)
            while self.sink.already_in_db(t0, object_id):
                t0 += self.config.max_delta_time
                t1 += self.config.max_delta_time
            df = self.source.extract_object(object_id, t0, t1)
            if df is None or df.empty:
                self.logger.info(f"Empty dataframe for {object_id} from {t0} to {t1}. Skipping...")
            else:
                # Check for duplicate columns and handle them
                if df.columns.duplicated().any():
                    duplicated_columns = df.columns[df.columns.duplicated()].tolist()
                    self.logger.warning(f"Duplicate columns found: {duplicated_columns}")
                    df = df.loc[:, ~df.columns.duplicated()]
                    self.logger.info("Duplicate columns removed.")
                
                # Handle NAType values and set column types
                df = self.handle_natype_and_set_column_types(df)
                
                self.sink.load_object(self.sink.table_id, df)
                self.logger.info(f"Wrote dataframe for {object_id} from {t0} to {t1}.")
            t0 = t1

    def handle_natype_and_set_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        # Example of handling NAType values and setting column types explicitly
        column_types = {
            'sol': 'float64',
            'presmax': 'float64',
            'presmin': 'float64',
            'horahrmax': 'float64',
            'horahrmin': 'float64',
            # Add other columns and their types as needed
        }
        for col, dtype in column_types.items():
            if col in df.columns:
                # Replace NAType with NaN for float conversion
                df[col] = df[col].replace({pd.NA: np.nan, 'NA': np.nan, None: np.nan, '': np.nan})
                # Clean the column by converting valid strings to numbers
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].astype(dtype)
        return df
