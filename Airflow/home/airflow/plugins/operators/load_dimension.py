from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    This Operator is an User Defined Operator.
    Operator is derived from Base Operator.
    Purpose of this operator is to due to load the dimensions 
    it deletes all the data in table if delete_ind is set as True and then loads the data into table
    '''
    ui_color = '#80BD9E'
    insert_sql_dimension = """
        INSERT INTO {}
        {} ; """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id = "",
                 table = "",
                 delete_ind = False,
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.table = table
        self.delete_ind = delete_ind
        self.sql_statement = sql_statement

    def execute(self, context):
        
        rsft_hook = PostgresHook(self.conn_id)

        if self.delete_ind:
            self.log.info("Clearing data from fact table {}".format(self.table))
            rsft_hook.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql_dimension.format(
            self.table,
            self.sql_statement
        )
        self.log.info("SQL STatement to executed {}".format(formatted_sql))
        rsft_hook.run(formatted_sql)
