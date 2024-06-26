from google.cloud import bigquery
from datetime import datetime, timezone
import pandas as pd
from typing import Protocol

class Sink(Protocol):
    def get_last_update_datetime(self, object_id: str) -> datetime:
        ...

    def load_object(self, object_id: str, df: pd.DataFrame) -> None:
        ...

class BigQuerySink:
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def get_last_update_datetime(self, object_id: str) -> datetime:
        query = f"""
            SELECT MAX(fecha) as last_update
            FROM `{self.dataset_id}.{self.table_id}`
            WHERE indicativo = '{object_id}'
        """
        query_job = self.client.query(query)
        print(query_job)
        result = query_job.result()
        for row in result:
            if row.last_update:
                return row.last_update
        return datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)  # Return a very old date if no data is found

    def already_in_db(self, t0: datetime, object_id):
        date = t0.strftime('%Y-%m-%d')
        query = f"""
            SELECT COUNT(*) AS count_rows
            FROM `{self.dataset_id}.{self.table_id}`
            WHERE fecha = '{date}';
        """
        query_job = self.client.query(query)
        result = query_job.result()
        for row in result:
            if row.count_rows > 0:
                return True
            else:
                return False
    
    def load_object(self, table_name: str, df: pd.DataFrame):
        client = bigquery.Client(project=self.project_id)
        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_name)

        # Convertimos el DataFrame a una tabla en BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Esperamos a que termine la carga

        print(f"Datos cargados en la tabla {table_name} en BigQuery.")

    def load_object_to_gbq(self, table_name: str, df: pd.DataFrame):
        client = bigquery.Client(project=self.project_id)
        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_name)

        # Convertir el DataFrame a una tabla en BigQuery usando to_gbq
        destination_table = f"{self.project_id}.{self.dataset_id}.{table_name}"

        df.to_gbq(destination_table=destination_table, if_exists='append', progress_bar=False)

        print(f"Datos cargados en la tabla {table_name} en BigQuery usando to_gbq.")