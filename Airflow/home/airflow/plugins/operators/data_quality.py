from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    This Operator is an User Defined Operator.
    Operator is derived from Base Operator.
    Purpose of this operator is to due to check the data quality
    if the table has records greater than 0.if not then it will result in error.
    '''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 conn_id = "",
                 table_name = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.table_name = table_name
    def execute(self, context):
        rsft_hook = PostgresHook(self.conn_id)
        count_of_records = rsft_hook.get_records("SELECT COUNT(*) FROM {}".format(self.table_name))
        if (len(count_of_records) < 1 or len(count_of_records[0]) < 1) or (count_of_records[0][0] <1):
            raise ValueError("Data quality check failed. {} returned no results/ 0 rows".format(self.table_name))
        self.log.info("Data quality on table {} check passed with {} count_of_records".format(self.table_name,count_of_records[0][0]))