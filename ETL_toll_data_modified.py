# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Sheldon Cooper',
    'start_date': days_ago(0),
    'email': ['sheldon.cooper@bazinga.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final assignment',
    schedule_interval=timedelta(days=1),
)
unzip_data=BashOperator(
    task_id=unzip_data,
    bash_command='sudo tar -xvf tolldata.tgz',
    dag=dag,

)
extract_data_from_csv=BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

extract_data_from_tsv=BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width=BashOperator(
    task_id='extract_data_from_fixed_width'
    bash_command = 'awk '{print substr($0, 53, 3) "," substr($0, 57, 5)}' payment-data.txt > fixed_width_data.csv',
    dag=dag,
)
consolidate_data=BashOperator(
    task_id='Consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,

)
transform_data=BashOperator(
    task_id='transform_data',
    bash_command='cut -f5  /home/project/airflow/dags/finalassignment/staging/extracted_data.csv  | tr [a-z] [A-Z]> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)
#Task_Pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data