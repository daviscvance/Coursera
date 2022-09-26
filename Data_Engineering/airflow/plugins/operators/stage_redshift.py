from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Operator for copying data from json S3 files to Redshift."""
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 target_table = '',
                 s3_bucket = '',
                 s3_key = '',
                 region = '',
                 format_option = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.format_option = format_option

    def execute(self, context):
        self.log.info(f'Start executing StageToRedshiftOperator for: "{self.target_table}"')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        
        self.log.info(f'Copying data from S3 to Redshift for: "{self.target_table}"')
        copy_sql_query = f"""
            COPY {self.target_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_credentials.access_key}'
            SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.format_option}'
        """
        redshift_hook.run(copy_sql_query)
        
        self.log.info(f'Copying log data to: "{self.target_table}" complete!')
