import math
from enum import Enum

from matplotlib import pyplot as plt
from pyspark.sql import DataFrame

import plotly.express as px
import plotly.figure_factory as ff
import numpy as np
import pandas as pd
import statistics as st
import plotly.io as pio

from spark import Spark

import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping, get_total_population, ECity, City

RESULT_DIR = 'results/infrastructure_compare'

class EStat(Enum):
    Med = 0,
    Mean = 1,


class Analyzer:
    _spark: Spark = None
    _stat: EStat = None

    _a_tags: dict = None
    _hexes: [] = None
    _city_name: str = None

    _all_sums: [] = None

    _nx_hexagon = 10

    def __init__(self, token_path: str, city: tuple, a_tags: dict) -> None:
        self._spark = Spark()
        self._a_tags = a_tags
        self._city_name = city[0].name

        _nx_hexagon = round((city[1].b_box.lon_max - city[1].b_box.lon_min / 0.02))

        px.set_mapbox_access_token(open(token_path).read())
        total_df = self._get_total_df(self._spark.get_data_frame(self._city_name), city[1])
        self._all_sums = []
        self._hexes = self._get_hexes(total_df)
        self._update_all_sums(total_df)

    def analysis(self, p_lim: int = 1, part_deg: int = 4, stat: EStat = EStat.Med) -> float:
        num_points = [0 for x in range(len(self._hexes))]
        self._update_num_points(num_points, p_lim, part_deg, stat)
        dfp = self._get_final_dataframe(num_points)

        fig = ff.create_hexbin_mapbox(
            data_frame=dfp, lat="lon", lon="lat",
            nx_hexagon=self._nx_hexagon, opacity=0.2, labels={"color": "Point Count"},
            color_continuous_scale="Viridis",
            min_count=2,
            original_data_marker=dict(size=4, opacity=0.4, color="deeppink")
        )
        pio.write_image(fig, RESULT_DIR + "/" + self._city_name + "_" + str(p_lim) + "_" + str(part_deg) +
                        "_" + stat.name + ".png", scale=1, width=1200, height=900)
        #fig.show()
        sum_ = 0
        num = 0
        for val in num_points:
            if val >= 2:
                num += 1
                sum_ += val
        return sum_ / num

    def _get_total_df(self, data_frame: DataFrame, city) -> DataFrame:
        query_string = ""
        flag = False
        for key, tag_group in self._a_tags.items():
            for tags in tag_group:
                for tag in tags:
                    if flag:
                        query_string += " or "
                    query_string += "(tags." + key.name + " == '" + tag + "')"
                    flag = True
        df = data_frame.filter(query_string).filter((data_frame.lat > city.b_box.lat_min) &
                                                    (data_frame.lat < city.b_box.lat_max) &
                                                    (data_frame.lon > city.b_box.lon_min) &
                                                    (data_frame.lon < city.b_box.lon_max))
        return df

    def _get_hexes(self, data_frame: DataFrame) -> []:
        lat_lot_df = data_frame.select("lat", "lon").toPandas()

        fig = ff.create_hexbin_mapbox(
            data_frame=lat_lot_df, lat="lat", lon="lon",
            nx_hexagon=self._nx_hexagon
        )
        #fig.show()

        data = fig.data
        temp = data[0].to_plotly_json().get('geojson').get('features')
        hex_coord = []
        for hex_ in temp:
            hex_coord.append(hex_.get('geometry').get('coordinates')[0])

        return hex_coord

    def _update_all_sums(self, data_frame: DataFrame):
        for key, tag_group in self._a_tags.items():
            for tags in tag_group:

                empty_rdd = self._spark.get_spark_session().sparkContext.emptyRDD()
                df = self._spark.get_spark_session().createDataFrame(empty_rdd, data_frame.schema)

                for tag in tags:
                    df = df.union(data_frame.filter("tags." + key.name + " == '" + tag + "'")).distinct()
                self._all_sums.append(self._get_hexes_sums(df))

    def _get_hexes_sums(self, df: DataFrame) -> ([], []):
        sums = [0 for x in range(len(self._hexes))]
        sums_wt_zeros = []

        pdf = df.select('lat', 'lon').toPandas()

        for i in range(pdf.shape[0]):
            #tmp = 0
            #for j in range(len(self._hexes)):
                #if self._is_in_hex(pdf.iloc[i]['lat'], pdf.iloc[i]['lon'], self._hexes[j]):
                    #tmp = j
                    #break

            ind = self._get_hex_ind(pdf.iloc[i]['lon'], pdf.iloc[i]['lat'])
            #if tmp != ind:
                #print(str(tmp), str(ind))
            sums[ind] += 1

        for sum_ in sums:
            if sum_ == 0:
                sums_wt_zeros.append(sum_)

        '''for hex_ in self._hexes:
            sum_ = 0
            #for li in df.select('lat', 'lon').toLocalIterator():
                #if self._is_in_hex(li['lat'], li['lon'], hex_):
                    #sum_ += 1
                    #continue
            for i in range(pdf.shape[0]):
                if self._is_in_hex(pdf.iloc[i]['lat'], pdf.iloc[i]['lon'], hex_):
                    sum_ += 1
            if sum_ != 0:
                sums_wt_zeros.append(sum_)
            sums.append(sum_)'''

        return sums, sums_wt_zeros

    def _update_num_points(self, num_points, p_lim: int, part_deg: int, stat: EStat):
        for sums, sums_wt_zeros in self._all_sums:
            if len(sums_wt_zeros) == 0:
                continue

            if stat == EStat.Med:
                stat_val = st.median(sums_wt_zeros)
            elif self._stat == EStat.Mean:
                stat_val = st.mean(sums_wt_zeros)
            else:
                stat_val = 0

            for i in range(len(sums)):
                for j in range(part_deg):
                    if sums[i] <= stat_val * p_lim / part_deg * j:
                        break
                    num_points[i] += 2

    def _get_final_dataframe(self, num_points: []) -> pd.DataFrame:
        arr = []
        lat = (self._hexes[0][0][0] + self._hexes[0][3][0]) / 2
        lon = (self._hexes[0][0][1] + self._hexes[0][3][1]) / 2
        arr.append([lat, lon])
        ind = round((len(self._hexes) + self._nx_hexagon) / (2 * self._nx_hexagon + 1) * (self._nx_hexagon + 1)) - 1
        lat = (self._hexes[ind][0][0] + self._hexes[ind][3][0]) / 2
        lon = (self._hexes[ind][0][1] + self._hexes[ind][3][1]) / 2
        arr.append([lat, lon])
        for i in range(len(self._hexes)):
            lat = (self._hexes[i][0][0] + self._hexes[i][3][0]) / 2
            lon = (self._hexes[i][0][1] + self._hexes[i][3][1]) / 2
            for j in range(num_points[i]):
                arr.append([lat, lon])

        return pd.DataFrame(np.array(arr), columns=['lat', 'lon'])

    def _get_hex_ind(self, lon: float, lat: float):
        lon_begin = self._hexes[0][5][0]
        #x_end = hexes[Nh * (W + 1) - 1][1][0]
        lat_begin = self._hexes[0][0][1]
        #y_end = hexes[Nh * (W + 1) - 1][3][1]

        Nh = round((len(self._hexes) + self._nx_hexagon) / (2 * self._nx_hexagon + 1))

        diam_1 = self._hexes[0][1][0] - self._hexes[0][5][0]
        side_1 = diam_1 / 2

        diam_2 = self._hexes[0][3][1] - self._hexes[0][0][1]
        side_2 = diam_2 / 2

        nx = math.floor((lon - lon_begin) / side_1)  # точка лежит в столбцах nx, nx-1
        ny = math.floor((lat - lat_begin) / side_2 / 3) * 2  # а вот тут сложнее

        if (math.floor((lat - lat_begin) / side_2)) % 3 == 2:  # точка лежит в строке ny + 1
            if nx % 2 == ny % 2 and nx != 0:
                i_hex = self._get_index(nx - 1, ny + 1, Nh)
            else:
                i_hex = self._get_index(nx, ny + 1, Nh)
            return i_hex
        # иначе точка лежит в строках ny, ny-1

        if (math.floor((lat - lat_begin) / side_2)) % 3 == 0:
            ny_tmp = ny - 1
        else:
            ny_tmp = ny + 1

        if nx % 2 == ny % 2:
            # [nx][ny] и [nx-1][ny-1]
            i_hex1 = self._get_index(nx, ny, Nh)
            if nx == 0 or ny == 0:
                return i_hex1
            i_hex2 = self._get_index(nx - 1, ny_tmp, Nh)
            if self._is_in_hex(lat, lon, self._hexes[i_hex1]):
                return i_hex1
            elif self._is_in_hex(lat, lon, self._hexes[i_hex2]):
                return i_hex2
            else:
                print("wtf")
                return i_hex1
        # тут должно быть определение, в какой из двух ячеек лежит точка
        else:
            # [nx][ny-1] и [nx-1][ny]
            if ny == 0:
                return self._get_index(nx - 1, ny, Nh)
            if nx == 0:
                return self._get_index(nx, ny_tmp, Nh)

            i_hex1 = self._get_index(nx, ny_tmp, Nh)
            i_hex2 = self._get_index(nx - 1, ny, Nh)
            if self._is_in_hex(lat, lon, self._hexes[i_hex1]):
                return i_hex1
            elif self._is_in_hex(lat, lon, self._hexes[i_hex2]):
                return i_hex2
            else:
                print("wtf")
                return i_hex1
        # тут должно быть определение, в какой из двух ячеек лежит точка

    def _get_index(self, a, b, Nh) -> int:
        if a % 2 == 0:
            return math.floor(a / 2) * Nh + math.floor(b / 2)
        else:
            return math.floor(a / 2) * (Nh - 1) + math.floor(b / 2) + Nh * (self._nx_hexagon + 1)

    @staticmethod
    def _is_in_hex(lat: float, lon: float, hex_: []) -> bool:
        for i in range(len(hex_) - 1):
            if np.linalg.det(
                    np.matrix(
                        [[hex_[i][0], hex_[i][1], 1], [hex_[i + 1][0], hex_[i + 1][1], 1], [lon, lat, 1]])) < 0:
                return False
        return True


class EGroupOfTags(Enum):
    amenity = 0,
    shop = 1,
    tourism = 2,
    public_transport = 3


AnalyzedTags = {
    EGroupOfTags.amenity: [["hospital", "clinic"],
                           ["school"],
                           ["place_of_worship"],
                           ["university"],
                           ["pharmacy"],
                           ["parking", "parking_entrance"],
                           ["cafe", "restaurant"],
                           ["police"],
                           ["fire_station"]],
    EGroupOfTags.shop: [["mall"]],
    EGroupOfTags.tourism: [["hotel"], ["museum"]],
    EGroupOfTags.public_transport: [["platform", "station"]]
}


def infrastructure_compare():
    values = []
    city_names = []

    for key, value in OverpassCityMapping.items():

        anal = Analyzer("mapbox_token", (key, value), AnalyzedTags)
        '''for p_lim in range(1, 6, 4):
            part_deg = 2
            while part_deg < 2 ** 7:
                anal.analysis(p_lim=p_lim, part_deg=part_deg, stat=EStat.Mean)
                part_deg = part_deg ** 2'''

        city_names.append(value.name)
        values.append(anal.analysis(stat=EStat.Mean))

    fig, ax = plt.subplots()
    ax.barh(city_names, values)
    plt.savefig(f'{RESULT_DIR}/compare.png', dpi=300)


if __name__ == '__main__':
    infrastructure_compare()
