from spark import Spark

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from functools import reduce
from matplotlib import pyplot as plt

import operator

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population, ECity


def print_counts(data_frame: DataFrame) -> None:
    amenity_elems = {
        'Больницы' : ['hospital', 'clinic'],
        'Церкви' : ['place_of_worship'],
        'Школы' : ['school'],
        'Университеты' : ['university'],
        'Аптеки' : ['pharmacy'],
        'Парковки' : ['parking', 'parking_entrance'],
        'Кафе' : ['cafe', 'restaurant'],
        'Полиция' : ['police'],
        'Пожарные станции' : ['fire_station']
    }

    for obj in amenity_elems:
        print(f'Amenity - {obj}: {data_frame.filter(reduce(operator.or_, [data_frame.tags.amenity == obj for obj in amenity_elems[obj]])).count()}')

    print(f'Shop - mall: {data_frame.filter(data_frame.tags.shop == "mall").count()}')
    print(f'Tourism - hotel: {data_frame.filter(data_frame.tags.tourism == "hotel").count()}')
    print(f'Tourism - museum: {data_frame.filter(data_frame.tags.tourism == "museum").count()}')
    print(f'Transport - platform: {data_frame.filter(reduce(operator.or_, [(data_frame.tags.public_transport == obj) for obj in ["platform", "station"]])).count()}')
    print(f'Office - it: {data_frame.filter(data_frame.tags.office == "it").count()}')
    print(f'Water - river: {data_frame.filter(data_frame.tags.water == "river").count()}')
    print(f'Sport - all: {data_frame.filter(col("tags.sport").isNotNull()).count()}')


def get_counts(data_frame: DataFrame) -> list:
    data_frame.show()
    amenity_elems = {
        'Больницы' : ['hospital', 'clinic'],
        'Церкви' : ['place_of_worship'],
        'Школы' : ['school'],
        'Университеты' : ['university'],
        'Аптеки' : ['pharmacy'],
        'Кафе' : ['cafe', 'restaurant'],
        'Полицейские участки' : ['police'],
        'Пожарные станции' : ['fire_station']
    }

    shop_elems = {
        'Торговые центры' : ['mall']
    }

    tourism_elems = {
        'Музеи' : ['museum'],
        'Отели' : ['hotel', 'hostel']
    }

    office_elems = {
        'IT компании' : ['it']
    }

    sports_elems = {
        'Спортивные объекты' : []
    }

    counts = [
        data_frame.filter(reduce(operator.or_, [data_frame.tags.amenity == obj for obj in amenity_elems[elem]])).count() for elem in amenity_elems
    ] + [
        data_frame.filter(reduce(operator.or_, [data_frame.tags.shop == obj for obj in shop_elems[elem]])).count() for elem in shop_elems
    ] + [
        data_frame.filter(reduce(operator.or_, [data_frame.tags.tourism == obj for obj in tourism_elems[elem]])).count() for elem in tourism_elems
    ] + [
        data_frame.filter(reduce(operator.or_, [data_frame.tags.office == obj for obj in office_elems[elem]])).count() for elem in office_elems
    ] + [
        data_frame.filter(col("tags.sport").isNotNull()).count() for _ in sports_elems
    ]

    return counts, [label for label in amenity_elems] + \
         [label for label in shop_elems] + \
            [label for label in tourism_elems] + \
                [label for label in office_elems] + \
                    [label for label in sports_elems]


def calculate_count():
    spark = Spark()

    counts, labels = get_counts(spark.get_union_data_frame([city.name for city in OverpassCityMapping]))
    plt.pie(counts, labels=labels, autopct='%1.1f%%', pctdistance=0.85, explode=[0.05 for _ in range(len(counts))])
    plt.show()

if __name__ == '__main__':
    calculate_count()
