from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id="",
                 table=""
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator IS implemented')
        redshift_hook = PostgresHook(postgres_conn_id = self.conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        
        logging.info(f"PASSED: Data quality on table {self.table} check.")