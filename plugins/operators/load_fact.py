from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from time import time as time_now

class LoadFactOperator(BaseOperator):
    """
    A generic custom Airflow operator that will allow more flexibility in 
    loading data into a fact table hosted in Amazon Redshift
    by allowing to provide customized relevant arguments values.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql_query="",
                 *args, **kwargs):
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
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info(f'Start {self.target_table} LoadDimensionOperator')

        # connect to Airflow PostgresHook for Redshift connection
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Loading fact records into "{self.target_table}" table.') 
        
        start_time = time_now()
        redshift_hook.run(self.sql_query)

        self.log.info('Loading fact records to "{target_table}" took: {took:.2f} seconds'.format(
                target_table=self.target_table,
                took=time_now() - start_time
            ))
        self.log.info(f'Loading fact records to "{self.target_table}" status: DONE!')
