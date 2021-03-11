from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, month
from pyspark.sql.types import DateType

def data_quality(s3_bucket, filename, columns):

    df = spark.read.load(s3_bucket + filename, header=True, inferSchema=True, format='parquet')

    for column in columns:
        num = df.where(col(column).isNull()).count()
        if num > 0:
            raise ValueError(f"Data quality check failed. Column: {column} returned {num} nulls")

if __name__ == "__main__":

    s3_bucket = "s3a://dend-jose/"
    filename = 'I94_data_transformed_2016.parquet'
    columns = ['arrival_date', 'origin_airport_code', 'stay_state']

    spark = SparkSession.builder.appName("Data quality").getOrCreate()

    data_quality(s3_bucket, filename, columns)