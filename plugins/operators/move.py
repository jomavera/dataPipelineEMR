from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
import os 

class LoadS3Operator(BaseOperator):
    """ Load data to S3

        Arguments:
            filename {str}: Filename of file to load
            s3_bucket {str}: Name of S3 bucket
            s3_key {str}: S3 key
        Returns:
            N/A
    """
    ui_color = '#358140'

    def __init__(self,
                filename='',
                s3_bucket='',
                s3_key='',
                replace=True,
                directory=False,
                *args, **kwargs):

        super(LoadS3Operator, self).__init__(*args, **kwargs)

        self.filename = filename
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace
        self.directory = directory

    def execute(self, context):
        s3 = S3Hook()
        if self.directory:
            for file_nme in os.listdir(self.filename):
                file_path = self.filename +'/'+ file_nme
                key_path = self.s3_key+'/' + file_nme
                self.log.info("Copying file:{} to S3 bucket:{}".format(file_path, self.s3_bucket))
                s3.load_file(filename=file_path, bucket_name=self.s3_bucket, replace=self.replace, key=key_path)
        else:
            self.log.info("Copying file:{} to S3 bucket:{}".format(self.filename, self.s3_bucket))
            s3.load_file(filename=self.filename, bucket_name=self.s3_bucket, replace=self.replace, key=self.s3_key)