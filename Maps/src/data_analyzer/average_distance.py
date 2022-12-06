from spark import Spark

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from functools import reduce
from matplotlib import pyplot as plt

from haversine import haversine
from numpy import Inf, mean, sum, arange

from timeit import default_timer

import bisect

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population, ECity

from imports import OverpassCityMapping, Visualizer

RESULT_DIR = 'results/average_dist'


class DistInfo:
    dist: float = 0.0
    count: int = 0

    def __init__(self, dist, count) -> None:
        self.dist = dist
        self.count = count


def calcalute_average_distance(data_frame: DataFrame, condition: Column):
    df = data_frame \
        .filter(condition) \
        .select(data_frame.lat, data_frame.lon) \

    localIterator = df.collect()

    def calc(coord, iterator):
        nearest_size = 10
        nearest_elems = [Inf for _ in range(nearest_size)]
        
        p1 = (coord.lat, coord.lon)

        for c in iterator:
            dist = haversine(p1, (c['lat'], c['lon']))

            if dist == 0.0:
                continue

            bisect.insort(nearest_elems, dist)
            del nearest_elems[-1]

        dist = mean([elem for elem in nearest_elems if elem != Inf])

        return dist if dist < 15.0 else None

    pair_dists = []

    for x in df.toLocalIterator():
        dist = calc(x, localIterator)

        if dist is not None:
            pair_dists.append(dist)

    return mean(pair_dists), df.count()


def calculate_all_average_distances(data_frame: DataFrame):
    elems = {
        'Больницы' : (data_frame.tags.amenity == 'hospital') | (data_frame.tags.amenity == 'clinic'),
        'Церкви' : data_frame.tags.amenity == 'place_of_worship',
        'Школы' : data_frame.tags.amenity == 'school',
        'Университеты' : data_frame.tags.amenity == 'university',
        'Аптеки' : data_frame.tags.amenity == 'pharmacy',
        'Кафе и рестораны' : (data_frame.tags.amenity == 'cafe') | (data_frame.tags.amenity == 'restaurant'),
        'Полицейские участки' : data_frame.tags.amenity == 'police',
        'Пожарные станции' : data_frame.tags.amenity == 'fire_station',
        'Торговые центры' : data_frame.tags.shop == 'mall',
        'Музеи' : data_frame.tags.tourism == 'museum',
        #'Парковки' : (data_frame.tags.amenity == 'parking') | (data_frame.tags.amenity == 'parking_entrance'),
        'Остановки общественного транспорта' : (data_frame.tags.public_transport == 'station') | (data_frame.tags.public_transport == 'platform'),
        'Отели' : (data_frame.tags.tourism == 'hotel') | (data_frame.tags.tourism == 'hostel')
    }

    dists = [DistInfo(*calcalute_average_distance(data_frame, elems[elem])) for elem in elems]

    return [dist.dist for dist in dists], [label for label in elems], [dist.count for dist in dists]


def average_distance():
    spark = Spark()
    visualizer = Visualizer(RESULT_DIR)
    visualizer.set_plot_color('green')

    start = default_timer()

    all_dists = {}
    labels = []

    for city in OverpassCityMapping:
        city_dists, labels, city_counts = calculate_all_average_distances(spark.get_data_frame(city.name))
        all_dists[city] = city_dists

        visualizer.plot_bar(
            city_dists,
            labels,
            OverpassCityMapping[city].name
            [f'Среднее расстояние между учреждениями одного типа. \n{OverpassCityMapping[city].name}', 'Расстояние (км)', 'Учреждение']
        )

        end = default_timer()

        print(f'Time({OverpassCityMapping[city].name}) : {end - start}')

    def sum_arrays(x, y):
        return [x_i + y_i for x_i, y_i in zip(x, y)]

    sum_dist = reduce(sum_arrays, [all_dists[city] for city in OverpassCityMapping])
    average_dist = [dist / len(OverpassCityMapping) for dist in sum_dist]

    visualizer.plot_bar(
        average_dist,
        labels,
        'Среднее по городам'
        [f'Среднее расстояние между учреждениями одного типа. \nВсе города', 'Расстояние (км)', 'Учреждение']
    )

    end = default_timer()

    print(f'Time: {end - start}')


if __name__ == '__main__':
    average_distance()
