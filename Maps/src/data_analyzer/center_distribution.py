from spark import Spark

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from functools import reduce
from matplotlib import pyplot as plt

from haversine import haversine
from numpy import floor

from timeit import default_timer

import pandas as pd
import seaborn as sns

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population, ECity, City

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


def plot_hist(dists: list, name: str, obj: str):
    df = pd.DataFrame(data=dists)

    quant_5, quant_25, quant_50, quant_75, quant_95 = df.quantile(0.05).item(), \
        df.quantile(0.25).item(), \
        df.quantile(0.50).item(), \
        df.quantile(0.75).item(), \
        df.quantile(0.95).item()

    quants = [
        [quant_5, 0.6, 0.16],
        [quant_25, 0.8, 0.26],
        [quant_50, 1.0, 0.36],
        [quant_75, 0.8, 0.46],
        [quant_95, 0.6, 0.56]
    ]

    fig, ax = plt.subplots(figsize=(6, 4))
    plt.style.use('bmh')
    ax.set_xlim([0, max(dists)])

    ax.hist(dists, density=True, bins=int(floor(1.72 * (len(dists) ** (1.0 / 3.0)))), alpha=0.50, edgecolor='black')
    sns.kdeplot(dists, bw_method=0.5, color='red')

    y_max = ax.get_ylim()[1]

    texts = ['5th', '25th', '50th', '75th', '95th']

    for q, text in zip(quants, texts):
        ax.axvline(q[0], alpha=q[1], ymax=q[2], linestyle=':')
        ax.text(q[0], (q[2] + 0.01) * y_max, text, color='black', ha='left', va='center')

    ax.grid(False)

    plt.xlabel('Расстояние (км)')
    plt.ylabel('Частота')
    plt.title(f'Распределение {obj.lower()} при удалении от центра. \n{name}')

    plt.savefig(f'{RESULT_DIR}/{name}_{obj}.png', dpi=300)


def plot_kdes(dists, labels, name):
    for dist, label in zip(dists, labels):
        sns.kdeplot(dist, bw_method=0.5, label=label)

    plt.xlim([0, 20])

    plt.legend()
    plt.show()


def center_distribution():
    spark = Spark()

    start = default_timer()

    for city in OverpassCityMapping:
        per_object_dists, labels = calculate_all_center_distribution(spark.get_data_frame(city.name), city)

        end = default_timer()

        print(f'Time({OverpassCityMapping[city].name}) : {end - start}')

        plot_kdes(per_object_dists, labels, OverpassCityMapping[city].name)

        break

        for dists, label in zip(per_object_dists, labels):
            plot_hist(dists, OverpassCityMapping[city].name, label)


    end = default_timer()

    print(f'Time: {end - start}')


if __name__ == '__main__':
    center_distribution()
