from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from time import time as time_now

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id:str="",
                 target_tables:list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_tables = target_tables

    def execute(self, context):
        self.log.info('Start performing data quality check')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failed_table = []
        
        for table in self.target_tables:

            self.log.info(f'Checking data quality on {table} table')
            
            start_time = time_now()

            table_records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')

            self.log.info('Data quality check took: {took:.2f} seconds'.format(
                    target_table=table,
                    took=time_now() - start_time
                ))
            self.log.info(f'Data quality check status: DONE!')
        
        if failed_table:
            raise ValueError('')

