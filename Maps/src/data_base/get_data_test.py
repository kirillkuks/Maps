from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from utils import get_data_frame, schema_diff

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population

from functools import reduce

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


def main():
    df = reduce(DataFrame.union, [get_data_frame(city.name) for city in OverpassCityMapping])
    df.printSchema()
    print_counts(df)

if __name__ == '__main__':
    main()
