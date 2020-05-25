from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    This Operator is an User Defined Operator.
    Operator is derived from Base Operator.
    Purpose of this operator is to due to load the stage tables using redshift s3 copy command  
    '''
    template_fields=("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        REGION 'US-WEST-2' 
        COMPUPDATE OFF STATUPDATE OFF
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
                
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.ignore_headers = ignore_headers


    def execute(self, context):
        self.log.info('StageToRedshiftOperator implemented ')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        rsft_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Clearing data from Stage table")
        rsft_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift staging table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        rsft_hook.run(formatted_sql)




