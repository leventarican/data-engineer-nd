from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info('LoadDimensionOperator IS implemented')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        redshift_hook.run(f"INSERT INTO {self.table} {self.sql_stmt}")