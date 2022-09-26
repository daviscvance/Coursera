from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 data_quality_checks = [
                     {
                         'target_table': '',
                         'expected_count': 0,
                         'condition': True,
                         'comparison': '',
                         'test_rationale': ''
                     }
                 ],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for i, dq in enumerate(self.data_quality_checks):
            target_table = dq.get('target_table')
            expected_count = dq.get('expected_count')
            condition = dq.get('condition')
            comparison = dq.get('comparison')
            rationale = dq.get('test_rationale')
            
            self.log.info(f'Checking data quality (#{i}) for table: "{target_table}" with condition: ' + \
                          f'"{condition}", Rationale: "{rationale}".')
            records = redshift_hook.get_records(
                f'SELECT COUNT(*) {comparison} {expected_count} FROM {target_table} WHERE {condition};')

            if not records[0][0]:
                raise ValueError(f'Data quality check (#{i}) for table: "{target_table}" with condition: ' + \
                                 f'"{condition}" FAILED. Expected "{comparison}{expected_count}" rows but ' + \
                                 f'got "{records[0][0]}". ' + \
                                 f'Modify test rationale: "{rationale}" to pass test.')
           
            self.log.info(f'Data quality check (#{i}) for table: "{target_table}" with condition: ' + \
                          f'"{condition}" and comparison: "{comparison}{expected_count}" PASSED.')