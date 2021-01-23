from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
    copy {} from '{}'
    ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}'
    region 'us-west-2' 
    timeformat as 'epochmillisecs'
    json '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 jsonpath_option="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id=conn_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.aws_credentials_id=aws_credentials_id
        self.jsonpath_option=jsonpath_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator IS implemented')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Copying data from S3 ({s3_path}) to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.jsonpath_option
        )
        redshift.run(formatted_sql)
        