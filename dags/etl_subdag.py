import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.check_has_row import CheckHasRowOperator
from operators.check_no_result import CheckNoResultOperator

def run_data_quality_checks(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    target_tables,
    *args, **kwargs):

    dag = DAG(
        dag_id=f'{parent_dag_name}.{task_id}',
        description='Run quality check if a table has result and contains rows.',
        *args,
        **kwargs)


    for table in target_tables:
        start_operator = DummyOperator(
            task_id=f'Begin_quality_check_on_{table}',  
            dag=dag)

        check_no_result = CheckNoResultOperator(
            task_id='Check_no_result_on_{}_table'.format(table),
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            target_table=table
        )

        check_has_row = CheckHasRowOperator(
            task_id='Check_has_row_on_{}_table'.format(table),
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            target_table=table
        )

        end_operator = DummyOperator(
            task_id=f'Complete_quality_check_on_{table}',  
            dag=dag)

        start_operator >> check_no_result 
        check_no_result >> check_has_row
        check_has_row >> end_operator

    return dag