from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

from functools import reduce

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class Spark:
    _spark: SparkSession = None
    _defaultSchema = StructType([
    StructField('lat', DoubleType(), True),
    StructField('lon', DoubleType(), True),
    StructField('tags', StructType([
        StructField(name, StringType(), True) for name in [
            'addr:city'
            'addr:housenumber',
            'addr:street',
            'amenity',
            'building',
            'name',
            'website',
            'contact:website',
            'shop',
            'phone',
            'opening_hours',
            'toilets',
            'colour',
            'station',
            'network',
            'tourism',
            'public_transport',
            'sport',
            'office',
            'water'
            ]
        ]))
    ])

    @staticmethod
    def get_spark_session() -> SparkSession:
        if Spark._spark is None:
            Spark()
            
        return Spark._spark

    def __init__(self) -> None:
        if Spark._spark is None:
            Spark._spark = SparkSession \
                .builder \
                .appName('Cities') \
                .master('local') \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()

    def get_data_frame(self, collection_name: str) -> DataFrame:
        return Spark._spark.read \
            .format('com.mongodb.spark.sql.DefaultSource') \
            .schema(self._defaultSchema) \
            .option('uri', f'mongodb://localhost:27017/maps.{collection_name}') \
            .load()

    def get_union_data_frame(self, collections: list) -> DataFrame:
        assert (len(collections) > 1)

        return reduce(DataFrame.union, [self.get_data_frame(collection_name) for collection_name in collections])


def schema_diff(df_1: DataFrame, df_2: DataFrame):
    s1 = Spark.get_spark_session().createDataFrame(df_1.dtypes, ["d1_name", "d1_type"])
    s2 = Spark.get_spark_session().createDataFrame(df_2.dtypes, ["d2_name", "d2_type"])
    difference = (
        s1.join(s2, s1.d1_name == s2.d2_name, how="outer")
        .where(s1.d1_type.isNull() | s2.d2_type.isNull())
        .select(s1.d1_name, s1.d1_type, s2.d2_name, s2.d2_type)
        .fillna("")
    )
    return difference
