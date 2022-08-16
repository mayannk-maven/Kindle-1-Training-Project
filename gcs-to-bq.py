#Importing the libraries
from airflow import DAG
from datetime import datetime,timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator


#Define DAG 
dag= DAG(dag_id= 'gcs-to-bq', start_date=datetime(2022,8,3), catchup=False, schedule_interval='@once')

#Using GoogleCloudStorageToBigQueryOperator to load csv from GCS to BQ
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
                            task_id="gcs_to_bq",
                            bucket="df-project-mw",
                            source_objects=["data.csv"],
                            source_format="CSV",
                            skip_leading_rows=1,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id="gcp",
                            schema_fields=[
                                {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'UnitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                                {'name': 'CustomerID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'}],
                            destination_project_dataset_table="df-project-358314.customer.E-Commerce",
                            dag=dag)

#Trigger Data cleaning dag using TriggerDagRunOperator
triggering_cleaning_process = TriggerDagRunOperator(
                                task_id="trigger_dependent_dag", 
                                trigger_dag_id="data-cleaning", 
                                wait_for_completion=True,
                                dag=dag)

#Initialing Dummy Operator
Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> gcs_to_bq >> triggering_cleaning_process >> End