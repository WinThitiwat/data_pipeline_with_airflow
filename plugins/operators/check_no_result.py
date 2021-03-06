from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from time import time as time_now

class CheckNoResultOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id:str="",
                 target_table:str="",
                 *args, **kwargs):

        super(CheckNoResultOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table

    def execute(self, context):
        self.log.info('Start performing data quality check')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        self.log.info(f'Checking data quality on "{self.target_table}" table')
        
        start_time = time_now()

        table_records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {self.target_table}')

        self.log.info('Data quality check took on "{target_table}": {took:.2f} seconds'.format(
                target_table=self.target_table,
                took=time_now() - start_time
            ))

        if len(table_records) < 1 or len(table_records[0]) < 1:
            self.log.warn(f'Data quality check on "{self.target_table}" status: FAILED!')
            self.log.warn(f'Data quality check response: "{self.target_table}" return no results')
            
            raise ValueError(f'Data quality check failed on {self.target_table}')
        
        self.log.info(f'Data quality check on "{self.target_table}" status: PASSED!')
        self.log.info(f'Number of results on "{self.target_table}" table: {len(table_records)}')
        self.log.info(f'Number of records inside the results on "{self.target_table}" table: {len([table_records[0]])}')

