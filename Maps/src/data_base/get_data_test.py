import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark import SparkContext
from maps_database import MapsDB
from pyspark.sql import SQLContext

def main():
    client = MapsDB()
    spark = SparkSession \
        .builder \
        .appName("Saint_Petersburg") \
        .master('local') \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/maps.Saint_Petersburg") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/maps.Saint_Petersburg") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
    data_frame = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://localhost:27017/maps.Saint_Petersburg") \
        .load()
    
    data_frame.printSchema()

    print(data_frame.filter(data_frame.tags.amenity == 'hospital').count())
    print(data_frame.filter(data_frame.tags.amenity == 'place_of_worship').count())
    print(data_frame.filter(data_frame.tags.amenity == 'school').count())
    print(data_frame.filter(data_frame.tags.amenity == 'university').count())
    print(data_frame.filter(data_frame.tags.shop == 'mall').count())
    print(data_frame.filter(data_frame.tags.amenity == 'pharmacy').count())
    print(data_frame.filter(data_frame.tags.amenity == 'parking').count())
    print(data_frame.filter(data_frame.tags.amenity == 'parking_entrance').count())
    print(data_frame.filter(data_frame.tags.tourism == 'hotel').count())
    print(data_frame.filter(data_frame.tags.tourism == 'museum').count())
    # транспорт, спорт, оффисы, вода, что-то ещё?

if __name__ == '__main__':
    main()
