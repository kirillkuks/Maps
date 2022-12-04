from spark import Spark

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from matplotlib import pyplot as plt

from numpy import arange

from timeit import default_timer

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population, ECity

RESULT_DIR = 'results/shops_count'


class MarketInfo:
    def __init__(self, name: str, count: int) -> None:
        self.name = name
        self.count = count


def calculate_shops_count(data_frame: DataFrame):
    df = data_frame.filter(data_frame.tags.shop == 'supermarket') \
            .filter(col('tags.name').isNotNull()) \
            .select('tags.name') 
            
    df = df.groupBy('name').count().filter('count > 10').sort('count', ascending=False)

    return [iter['count'] for iter in df.toLocalIterator()], [iter['name'] for iter in df.toLocalIterator()]


def plot_bar(counts, labels, name):
    fig, ax = plt.subplots()
    width = 0.75
    ind = arange(len(counts))
    ax.barh(ind, counts, 0.75, color='#E94C8D')
    ax.set_yticks(ind + width / 2)
    ax.set_yticklabels(['' for _ in range(len(counts))], minor=False)

    print(labels)

    for bar, label, dist in zip(ax.patches, labels, counts):
        ax.text(0.1, bar.get_y() + bar.get_height() / 2, f'{label}, {round(dist, 2)}', color='black', ha='left', va='center')        

    plt.xlabel('Количество магазинов')
    plt.ylabel('Магазины')
    plt.title(f'Распределение по количеству мгазинов. \n{name}')

    plt.margins(0, 0.05)
    plt.savefig(f'{RESULT_DIR}/{name}.png', dpi=300)


def shops_count():
    spark = Spark()

    start = default_timer()

    counts, names = calculate_shops_count(spark.get_union_data_frame([city.name for city in OverpassCityMapping]))

    plot_bar(counts[:10], names[:10], 'Все города')

    for city in OverpassCityMapping:
        counts, names = calculate_shops_count(spark.get_data_frame(city.name))

        plot_bar(counts[:10], names[:10], OverpassCityMapping[city].name)

    end = default_timer()

    print(f'Time: {end - start}')



if __name__ == '__main__':
    shops_count()
