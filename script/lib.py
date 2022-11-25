


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