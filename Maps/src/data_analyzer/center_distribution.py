from spark import Spark

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from functools import reduce
from matplotlib import pyplot as plt

from haversine import haversine
from numpy import floor

from timeit import default_timer

import seaborn as sns

from imports import OverpassCityMapping, ECity, City, Visualizer

RESULT_DIR = 'results/center_distribution'


def calculate_center_distribution(data_frame: DataFrame, condition: Column, city: City):
    df = data_frame.filter(condition) \
                    .select(data_frame.lat, data_frame.lon)

    localIterator = df.toLocalIterator()
    cityCenter = city.center

    dists = [haversine((cityCenter.lat, cityCenter.lon), (coord.lat, coord.lon)) for coord in localIterator]

    return [dist for dist in dists if dist < 20.0]


def calculate_all_center_distribution(data_frame: DataFrame, city: ECity):
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
        'Парковки' : (data_frame.tags.amenity == 'parking') | (data_frame.tags.amenity == 'parking_entrance'),
        'Остановки общественного транспорта' : (data_frame.tags.public_transport == 'station') | (data_frame.tags.public_transport == 'platform'),
        'Отели' : (data_frame.tags.tourism == 'hotel') | (data_frame.tags.tourism == 'hostel')
    }

    return [calculate_center_distribution(data_frame, elems[elem], OverpassCityMapping[city]) for elem in elems], \
            [label for label in elems]


def plot_kdes(dists, labels, name):
    for dist, label in zip(dists, labels):
        sns.kdeplot(dist, bw_method=0.5, label=label)

    plt.xlim([0, 20])

    plt.legend()
    plt.show()


def center_distribution():
    spark = Spark()
    visualizer = Visualizer(RESULT_DIR)

    start = default_timer()

    for city in OverpassCityMapping:
        per_object_dists, labels = calculate_all_center_distribution(spark.get_data_frame(city.name), city)

        end = default_timer()

        print(f'Time({OverpassCityMapping[city].name}) : {end - start}')

        for dists, label in zip(per_object_dists, labels):
            visualizer.plot_hist(
                dists,
                OverpassCityMapping[city].name,
                label,
                [f'Распределение {label.lower()} при удалении от центра. \n{OverpassCityMapping[city].name}', 'Расстояние (км)', 'Частота']
            )


    end = default_timer()

    print(f'Time: {end - start}')


if __name__ == '__main__':
    center_distribution()
