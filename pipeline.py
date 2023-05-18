project.py
from airflow.decorators import dag, task
from airflow import DAG

from datetime import datetime, timedelta


from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import sys 

sys.path.append('/opt/airflow/includes')

from emp_dim_insert_update import join_and_detect_new_or_changed_rows

from queries import *
from queries import INSERT_INTO_DWH_EMP_DIM
from queries import UPDATE_DWH_EMP_DIM

@task.branch(task_id="branch_task")
def branch_func(ids_to_updatee):
    if ids_to_updatee == '':
        return ["dummy_task"]
    else  :
        return ["snowflake_update"]


with DAG("project" , start_date=datetime(2023,5,14),schedule='@hourly') as dag:

    sql_to_s3_task_emp_sal = SqlToS3Operator(
    task_id="sql_to_s3_task_emp_sal",
    sql_conn_id='pst_conn',
    aws_conn_id='AWSconn' , 
    query='select * from finance.emp_sal',
    s3_bucket='staging.emp.data',
    s3_key='ibrahim_employee_salary.csv',
    replace=True,
    )
    
    sql_to_s3_task_emp_details = SqlToS3Operator(
    task_id="sql_to_s3_task_emp_details",
    sql_conn_id='pst_conn',
    aws_conn_id='AWSconn' , 
    query='select * from hr.emp_details',
    s3_bucket='staging.emp.data',
    s3_key='ibrahim_employee_details.csv',
    replace=True,
    )
    
    snowflake_update = SnowflakeOperator(
    task_id='snowflake_update',
    sql=UPDATE_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows",key="ids_to_update") }}'),
    snowflake_conn_id='iti_snow'
    )
    
    snowflake_insert = SnowflakeOperator(
    task_id='insert',
    sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows",key="rows_to_insert") }}'),
    snowflake_conn_id='iti_snow',
    trigger_rule="none_failed"
    )
    
    branch_op = branch_func("{{ ti.xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='ids_to_update') }}")
    
    dummy_task=DummyOperator(task_id="dummy_task")

    [sql_to_s3_task_emp_sal, sql_to_s3_task_emp_details] >> join_and_detect_new_or_changed_rows() >> branch_op>>[snowflake_update,dummy_task]>>snowflake_insert





