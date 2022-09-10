from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Helps transfer data from loaded log files into fact tables."""
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 target_table = '',
                 sql = '',
                 refresh_table = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql
        self.refresh_table = refresh_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.refresh_table:
            self.log.info(f'Refreshing table: "{self.target_table}".')
            redshift_hook.run(f"""TRUNCATE TABLE {self.target_table}; COMMIT;""")
        
        self.log.info(f'Loading facts into table: "{self.target_table}".')
        redshift_hook.run(f"""{self.sql}; COMMIT;""")
        self.log.info(f'Completed loading facts to table: "{self.target_table}"!')