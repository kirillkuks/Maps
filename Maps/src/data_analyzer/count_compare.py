from spark import Spark

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from matplotlib import pyplot as plt

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population


def print_counts(data_frame: DataFrame) -> None:
    print(f'Amenity - hospital: {data_frame.filter((data_frame.tags.amenity == "hospital") | (data_frame.tags.amenity == "clinic")).count()}')
    print(f'Amenity - place_of_worship: {data_frame.filter(data_frame.tags.amenity == "place_of_worship").count()}')
    print(f'Amenity - school: {data_frame.filter(data_frame.tags.amenity == "school").count()}')
    print(f'Amenity - university: {data_frame.filter(data_frame.tags.amenity == "university").count()}')
    print(f'Shop - mall: {data_frame.filter(data_frame.tags.shop == "mall").count()}')
    print(f'Amenity - pharmacy: {data_frame.filter(data_frame.tags.amenity == "pharmacy").count()}')
    print(f'Amenity - parking: {data_frame.filter((data_frame.tags.amenity == "parking") | (data_frame.tags.amenity == "parking_entrance")).count()}')
    print(f'Tourism - hotel: {data_frame.filter(data_frame.tags.tourism == "hotel").count()}')
    print(f'Tourism - museum: {data_frame.filter(data_frame.tags.tourism == "museum").count()}')
    print(f'Transport - platform: {data_frame.filter((data_frame.tags.public_transport == "platform") | (data_frame.tags.public_transport == "station")).count()}')
    print(f'Office - it: {data_frame.filter(data_frame.tags.office == "it").count()}')
    print(f'Water - river: {data_frame.filter(data_frame.tags.water == "river").count()}')
    print(f'Sport - all: {data_frame.filter(col("tags.sport").isNotNull()).count()}')
    print(f'Amenity - cafe, restaraunt : {data_frame.filter((data_frame.tags.amenity == "cafe") | (data_frame.tags.amenity == "restaurant")).count()}')
    print(f'Amenity - police : {data_frame.filter((data_frame.tags.amenity == "police")).count()}')
    print(f'Amenity - fire_station : {data_frame.filter(data_frame.tags.amenity == "fire_station").count()}')


def get_counts(data_frame: DataFrame) -> list:
    return [
        data_frame.filter((data_frame.tags.amenity == "hospital") | (data_frame.tags.amenity == "clinic")).count(),
        data_frame.filter(data_frame.tags.amenity == "place_of_worship").count(),
        data_frame.filter(data_frame.tags.amenity == "school").count(),
        data_frame.filter(data_frame.tags.amenity == "university").count(),
        data_frame.filter(data_frame.tags.shop == "mall").count(),
        data_frame.filter(data_frame.tags.amenity == "pharmacy").count(),
        data_frame.filter((data_frame.tags.amenity == "parking") | (data_frame.tags.amenity == "parking_entrance")).count(),
        data_frame.filter(data_frame.tags.tourism == "hotel").count(),
        data_frame.filter(data_frame.tags.tourism == "museum").count(),
        data_frame.filter((data_frame.tags.public_transport == "platform") | (data_frame.tags.public_transport == "station")).count(),
        data_frame.filter(data_frame.tags.office == "it").count(),
        data_frame.filter(data_frame.tags.water == "river").count(),
        data_frame.filter(col("tags.sport").isNotNull()).count(),
        data_frame.filter((data_frame.tags.amenity == "cafe") | (data_frame.tags.amenity == "restaurant")).count(),
        data_frame.filter((data_frame.tags.amenity == "police")).count(),
        data_frame.filter(data_frame.tags.amenity == "fire_station").count()
    ]


def calculate_count():
    spark = Spark()

    df1 = spark.get_data_frame('Saint_Petersburg')
    df2 = spark.get_data_frame('Saint_Petersburg')

    #plt.pie(get_counts(spark.get_union_data_frame([city.name for city in OverpassCityMapping])))
    #plt.show()

if __name__ == '__main__':
    calculate_count()
