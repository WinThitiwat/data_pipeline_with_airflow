from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# deprecated
from airflow.contrib.hooks.aws_hook import AwsHook
# replacement of the contrib.hook.aws_hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook 

from time import time as time_now

class StageToRedshiftOperator(BaseOperator):
    """
    A generic custom operator that will allow more flexibility in 
    loading data from S3 into Redshift data warehouse by allowing to 
    provide customized relevant arguments values.
    """
    ui_color = '#358140'
    
    copy_sql_query = """
        COPY {target_table}
        FROM '{source}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        IGNOREHEADER {ignore_header}
        DELIMITER '{delimiter}'
        FORMAT AS JSON '{format_option}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 target_table='',
                 s3_bucket='',
                 s3_key='',
                 delimiter=',',
                 ignore_header=1,
                 format_option="auto",
                 *args, **kwargs):
        """
        :param redshift_conn_id
        :param aws_credentials_id
        :param target_table
        :param s3_bucket
        :param s3_key
        :param delimiter
        :param ignore_header
        :param format_option
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_header = ignore_header
        self.format_option = format_option

    def execute(self, context):
        self.log.info('Start executing StageToRedshiftOperator')

        # get all relevant connection hook to run command
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        render_key = self.s3_key.format(
            year=context['execution_date.year'],
            month=context['execution_date.month'],
            full_date=context["ds"])

        s3_path = f's3://{self.s3_bucket}/{render_key}'

        formatted_copy_query = StageToRedshiftOperator.copy_sql_query.format(
            target_table=self.target_table,
            source=s3_path,
            access_key_id=aws_credentials.access_key,
            secret_access_key=aws_credentials.secret_key,
            ignore_header=self.ignore_header,
            delimiter=self.delimiter,
            format_option=self.format_option,
        )

        self.log.info(f'Copying data to "{self.target_table}" staging table.') 
        
        start_time = time_now()
        redshift_hook.run(formatted_copy_query)

        self.log.info('Copying data to "{self.target_table}" took: {took:2f} seconds'.format(
                target_table=self.target_table,
                took=time_now() - start_time
            ))
        self.log.info(f'Copying data to "{self.target_table}" status: DONE!')












