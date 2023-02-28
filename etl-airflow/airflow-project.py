from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator

#This code has been tested on Airflow 2.3.3.
default_args = {
    'owner': 'James T. Kirk',
    'start_date': days_ago(0),
    'email': 'jamestkirk@exemplo.com.br',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id = 'ETL_toll_data',
          description = 'Apache Airflow Final Assignment',
          default_args = default_args,
          schedule_interval = timedelta(days = 1),
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = ('tar -xzf '
                    '/home/project/airflow/dags/finalassignment/tolldata.tgz '
                    '-C /home/project/airflow/dags/finalassignment/staging'
    ),
    dag = dag,
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = ('cut -d"," -f1-4 '
                    '< /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv '
                    '> /home/project/airflow/dags/finalassignment/staging/csv_data.csv'
    ),
    dag = dag,
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = ('cut -f5-7 --output-delimiter="," '
                    '< /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv '
                    '> /home/project/airflow/dags/finalassignment/staging/tsv_data.csv'
    ),
    dag = dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = ('cut -c 59-61,63-67 --output-delimiter="," '
                    '< /home/project/airflow/dags/finalassignment/staging/payment-data.txt '
                    '> /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv'
    ),
    dag = dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = ('paste -d"," '
                    '/home/project/airflow/dags/finalassignment/staging/csv_data.csv '
                    '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv '
                    '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv '
                    '> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv'
    ),
    dag = dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = ('''tr -d '\\r' '''
                    '''< /home/project/airflow/dags/finalassignment/staging/extracted_data.csv '''
                    '''| awk 'BEGIN{FS=","; OFS=","} {$4 = toupper($4)}1' '''
                    '''> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv'''
    ),
    dag = dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
