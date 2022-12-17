
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DecimalType, TimestampType, DataType, IntegerType
from pyspark.sql.functions import col, lit, trim, substring, concat, udf, upper, initcap
from datetime import datetime
import os

LAKE_HOME = os.getenv("LAKE_HOME", "/spark/home")

def create_sk(spark, df, key_column_name, table_name):
    sk = {}
    sk = df.select(col(key_column_name).alias("key")).rdd.zipWithIndex()
    new_sk = sk.map(lambda row: list(row[0]) + [row[1] + 1])
    new_sk_map = new_sk.collectAsMap()

    sk_schema = \
        StructType(
            [StructField('key', StringType(), True),
            StructField('SK', LongType(), True)]
        )

    sk_frame = spark.createDataFrame(new_sk, sk_schema)
    sk_frame.write.mode('overwrite').csv('{}/dataset/e-commerce/02_surrogate_key/sk_{}.csv'.format(LAKE_HOME, table_name), header=True)

    return new_sk_map

def locate_sk(mapping: dict):
    return udf(lambda x: mapping.get(x), IntegerType())


def map_sk(spark, table_name):
    sk_table = spark.read.csv('{}/dataset/e-commerce/02_surrogate_key/sk_{}.csv'.format(LAKE_HOME, table_name), header=True)
    dict_sk = sk_table.rdd.map(lambda x: (x[0], int(x[1]))).collectAsMap()
    return dict_sk