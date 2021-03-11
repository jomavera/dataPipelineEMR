from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ Data Quality Check for Tables
        Arguments:
            tables {list}:  List of tables to be checked
            redshift_conn_id {str}: redshift connection id
            sql_check {str}: SQL code to make the check
        Returns:
            N/A
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                tables = [],
                redshift_conn_id = '',
                sql_check='',
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.sql_check = sql_check

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f'Checking Table: {table}')
            column = table[:-1]+'_id'
            sql_format = self.sql_check.format(column, table)
            records = redshift.get_records(sql_format)

            num_nulls = records[0][0]   
            if num_nulls > 0:
                raise ValueError(f"Data quality check failed. Table: {table} returned {num_nulls} nulls")
            
            self.log.info(f"Data quality on Table: {table} check passed with 0 nulls")