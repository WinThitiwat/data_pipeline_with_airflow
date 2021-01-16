from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from time import time as time_now

class CheckNoResultOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id:str="",
                 target_tables:list=[],
                 *args, **kwargs):

        super(CheckNoResultOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_tables = target_tables

    def execute(self, context):
        self.log.info('Start performing data quality check')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failed_table = []
        
        for table in self.target_tables:

            self.log.info(f'Checking data quality on "{table}" table')
            
            start_time = time_now()

            table_records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')

            self.log.info('Data quality check took on "{target_table}": {took:.2f} seconds'.format(
                    target_table=table,
                    took=time_now() - start_time
                ))

            if len(table_records) < 1 or len(table_records[0]) < 1:
                failed_table.append(table)

                self.log.warn(f'Data quality check on "{table}" status: FAILED!')
                self.log.warn(f'Data quality check response: "{table}" return no results')

            else:
                self.log.info(f'Data quality check on "{table}" status: PASSED!')
        
        if failed_table:
            raise ValueError('Data quality check failed on {failed_table}'.format(
                                failed_table=', '.join(failed_table)
                            ))

