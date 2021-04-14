from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from time import time as time_now

class LoadDimensionOperator(BaseOperator):
    """
    A generic custom Airflow operator that will allow more flexibility in 
    loading data into a dimensional table hosted in Amazon Redshift
    by allowing to provide customized relevant arguments values.
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id:str='',
                 target_table:str='',
                 sql_query:str='',
                 delete_records_before_load:bool=False,
                 *args, **kwargs) -> None :
        """
        Constructs all the necessary attributes for the 
        LoadDimensionOperator object.

        :param redshift_conn_id: the Amazon Redshift connection ID created in
        the Airflow Connections
        :type redshift_conn_id: str

        :param target_table: the Amazon Redshift target table name
        :type target_table: str

        :param sql_query: SQL query to be compiled and loaded to the target table
        :type sql_query: str

        :param delete_records_before_load: If True, the Operator will 
            truncate the target table before the load. Default is False
        :type delete_records_before_load: bool
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query
        self.delete_records_before_load = delete_records_before_load

    def execute(self, context):
        self.log.info(f'Start {self.target_table} LoadDimensionOperator')
        
        # connect to Airflow PostgresHook for Redshift connection
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # check if empty table before loading is needed
        if self.delete_records_before_load:
            
            self.log.info(f'Deleting records in the "{self.target_table}" table.') 
            
            delete_records = f'DELETE FROM {self.target_table}'
            start_time = time_now()
            redshift_hook.run(delete_records)

            self.log.info('Delete {target_table} records took {took}'.format(
                target_table=self.target_table,
                took=time_now() - start_time
            ))
            self.log.info(f'Delete {self.target_table} records completed!')

        self.log.info(f'Loading dimension records into "{self.target_table}" table.') 
        
        start_time = time_now()
        redshift_hook.run(self.sql_query)

        self.log.info('Loading dimension records to "{target_table}" took: {took:.2f} seconds'.format(
                target_table=self.target_table,
                took=time_now() - start_time
            ))
        self.log.info(f'Loading dimension records to "{self.target_table}" status: DONE!')

