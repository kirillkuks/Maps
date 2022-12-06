from spark import Spark

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from matplotlib import pyplot as plt

from numpy import arange

from timeit import default_timer

from imports import OverpassCityMapping, ECity, Visualizer

RESULT_DIR = 'results/elems_count'


class MarketInfo:
    def __init__(self, name: str, count: int) -> None:
        self.name = name
        self.count = count


def calculate_counts(data_frame: DataFrame, condition: Column, select_condition: str = 'tags.name', group_by_condition: str = 'name'):
    df = data_frame.filter(condition) \
            .filter(col(select_condition).isNotNull()) \
            .select(select_condition) 
            
    df = df.groupBy(group_by_condition).count().sort('count', ascending=False)

    return [iter['count'] for iter in df.toLocalIterator()], [iter[group_by_condition] for iter in df.toLocalIterator()]



def calculate_shops_count(data_frame: DataFrame):
    return calculate_counts(data_frame, data_frame.tags.shop == 'supermarket')

def calculate_fast_food_count(data_frame: DataFrame):
    return calculate_counts(data_frame, data_frame.tags.amenity == 'fast_food')

def calcualte_sports_count(data_frame: DataFrame):
    return calculate_counts(data_frame, col('tags.sport').isNotNull(), 'tags.sport', 'sport')


def elems_count(spark: Spark, visualizer: Visualizer, calculate_func, labels: list):
    start = default_timer()

    counts, names = calculate_func(spark.get_union_data_frame([city.name for city in OverpassCityMapping]))

    visualizer.plot_bar(
        counts[:10],
        names[:10],
        'Все города',
        [f'{labels[0]}. \nВсе города', labels[1], labels[2]]
    )

    for city in OverpassCityMapping:
        counts, names = calculate_func(spark.get_data_frame(city.name))

        visualizer.plot_bar(
            counts[:10],
            names[:10],
            OverpassCityMapping[city].name,
            [f'{labels[0]}. \n{OverpassCityMapping[city].name}', labels[1], labels[2]]
        )

    end = default_timer()

    print(f'Time: {end - start}')


def shops_count(spark: Spark):
    visualizer = Visualizer(f'{RESULT_DIR}/shops')
    visualizer.set_plot_color('#E94C8D')

    elems_count(
        spark,
        visualizer,
        calculate_shops_count,
        ['Распределение по количеству магазинов', 'Количество магазинов', 'Магазины']
    )


def fast_food_count(spark: Spark):
    visualizer = Visualizer(f'{RESULT_DIR}/fast_food')
    visualizer.set_plot_color('#FFE400')

    elems_count(
        spark,
        visualizer,
        calculate_fast_food_count,
        ['Распределение по количеству ресторанов фастфуда', 'Количество ресторанов', 'Ресторан']
    )


def sports_count(spark: Spark):
    visualizer = Visualizer(f'{RESULT_DIR}/sports')
    visualizer.set_plot_color('#3AAFA9')

    elems_count(
        spark,
        visualizer,
        calcualte_sports_count,
        ['Распределение по количеству спортивных объектов', 'Количество объектов', 'Вид спорта']
    )


if __name__ == '__main__':
    spark = Spark()

    print('Shops:')
    shops_count(spark)

    print('Fast food:')
    fast_food_count(spark)

    print('Sport:')
    sports_count(spark)
