from google.cloud import bigquery

def create_table_with_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Definir los campos del esquema basado en los metadatos
    schema = [
        bigquery.SchemaField("fecha", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("indicativo", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("nombre", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("provincia", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("altitud", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("tmed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("prec", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tmin", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("horatmin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tmax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("horatmax", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dir", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("velmedia", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("racha", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("horaracha", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("horapresmax", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("horapresmin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hrmedia", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("hrmax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("horahrmax", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hrmin", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("horahrmin", "STRING", mode="NULLABLE")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)  # Crear la tabla en BigQuery
    print(f"Tabla {table_id} creada en el dataset {dataset_id}.")

# Reemplaza estos valores con los correspondientes a tu proyecto y tabla
#create_table_with_schema("tu-proyecto", "data_etl", "aemet")