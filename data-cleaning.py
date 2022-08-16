#Importing the libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import pandas_gbq
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator


#Define DAG 
dag= DAG(dag_id= 'data-cleaning', start_date = datetime.today(), catchup=False, schedule_interval='@once')

#Query to load the table from BQ Dataset
query_1 = '''SELECT * FROM `df-project-358314.customer.E-Commerce`;'''

#Creating a Funtion for removing negitive values & Null values
def cleaning1():
    df = pd.read_gbq(query_1) #Converting the table to pandas dataframe

    index1 = df[df.Quantity < 0].index #Filtering the negative values from Quantity column in the dataset
    df.drop(index1, axis=0, inplace=True) #Dropping the negative values from Quantity column in the dataset

    index2 = df[df.CustomerID.isnull()].index #Filtering the null values `from CustomerID column in the dataset
    df.drop(index2, axis=0, inplace=True) #Dropping out the null values from CustomerID column in the dataset

    #Converting pandas dataframe to BQ Dataset
    pandas_gbq.to_gbq(df, destination_table='customer.Data_Cleaning1', project_id='df-project-358314', if_exists='replace')

#Running the tasks
Data_Cleaning1 = PythonOperator(task_id='Data_Cleaning1', python_callable=cleaning1 , dag=dag)

#Query to convert InvoiceDate to DateTime format
query_2 = '''   SELECT
                InvoiceNo, StockCode, Description, Quantity,
                PARSE_DATETIME("%m/%d/%Y %H:%M", InvoiceDate) AS InvoiceDate,
                UnitPrice, CustomerID, Country
                FROM `df-project-358314.customer.Data_Cleaning1`;
                '''

#Running the tasks
Data_Cleaning2 = BigQueryOperator(  task_id='Data_Cleaning2', 
                                    destination_dataset_table= "df-project-358314.customer.Data_Cleaning2",
                                    sql=query_2,
                                    use_legacy_sql=False,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    bigquery_conn_id='bigquery_default',
                                    dag=dag)


#Query to remove incomplete data
query_3 = '''   SELECT InvoiceNo, StockCode, Description, Quantity,InvoiceDate, UnitPrice, CustomerID, Country
                FROM (SELECT    *,
                                CAST(EXTRACT(day FROM InvoiceDate) AS int64) AS Day,
                                FORMAT_DATE('%m-%Y', InvoiceDate) AS Month      
                FROM `df-project-358314.customer.Data_Cleaning2`) AS T1
                INNER JOIN (SELECT 
                                Month 
                            FROM 
                                (SELECT *,
                                        CAST(EXTRACT(day FROM InvoiceDate) AS int64) AS Day,
                                        FORMAT_DATE('%m-%Y', InvoiceDate) AS Month      
                                FROM `df-project-358314.customer.Data_Cleaning2`)
                GROUP BY Month
                HAVING COUNT(DISTINCT Day)  >= 24) AS T2
                ON T1.MONTH = T2.MONTH
                WHERE (Quantity*UnitPrice)>0;
                '''

#Running the tasks
Data_Cleaning3 = BigQueryOperator(  task_id='Data_Cleaning3', 
                                    destination_dataset_table= "df-project-358314.customer.Data_Cleaning3",
                                    sql=query_3,
                                    use_legacy_sql=False,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    bigquery_conn_id='bigquery_default',
                                    dag=dag)

#Query to create table ONLINE_RETAIL
query_4 = '''   SELECT
                InvoiceNo, StockCode, Description, Quantity,InvoiceDate,
                UnitPrice, CustomerID, Country, (Quantity*UnitPrice) AS ItemTotal
                FROM
                `df-project-358314.customer.Data_Cleaning3`;'''

#Running the tasks
Create_Table1 = BigQueryOperator(   task_id='Create_Table1', 
                                    destination_dataset_table= "df-project-358314.customer.ONLINE_RETAIL",
                                    sql=query_4,
                                    use_legacy_sql=False,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    bigquery_conn_id='bigquery_default',
                                    dag=dag)


#Query to create table CUSTOMER_SUMMARY
query_5=''' SELECT
                CustomerID,
                ROUND(SUM(ItemTotal),2) AS TotalSales,
                COUNT(InvoiceNo) AS OrderCount,
                ROUND(SUM(ItemTotal)/COUNT(InvoiceNo),2) AS AvgOrderValue
            FROM `df-project-358314.customer.ONLINE_RETAIL`
            GROUP BY CustomerID
            ORDER BY OrderCount DESC;'''

#Running the tasks
Create_Table2 = BigQueryOperator(   task_id='Create_Table2', 
                                    destination_dataset_table= "df-project-358314.customer.CUSTOMER_SUMMARY",
                                    sql=query_5,
                                    use_legacy_sql=False,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    bigquery_conn_id='bigquery_default',
                                    dag=dag)

#Query to create table SALES_SUMMARY
query_6= '''    SELECT 
                        Country, 
                        TotalSales, 
                        ROUND((TotalSales*100/S),2) AS PercentofCountrySales 
                FROM
                    (SELECT Country, ROUND(SUM(ItemTotal),2) AS TotalSales
                    FROM `df-project-358314.customer.ONLINE_RETAIL`
                    GROUP BY 1)
                CROSS JOIN (SELECT SUM(ItemTotal) AS S FROM `df-project-358314.customer.ONLINE_RETAIL`)
                ORDER BY PercentofCountrySales DESC;'''

#Running the tasks
Create_Table3 = BigQueryOperator(   task_id='Create_Table3', 
                                    destination_dataset_table= "df-project-358314.customer.SALES_SUMMARY",
                                    sql=query_6,
                                    use_legacy_sql=False,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    bigquery_conn_id='bigquery_default',
                                    dag=dag)


Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> Data_Cleaning1 >> Data_Cleaning2 >> Data_Cleaning3 >> [Create_Table1, Create_Table2, Create_Table3] >> End