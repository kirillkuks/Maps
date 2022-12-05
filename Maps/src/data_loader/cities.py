from enum import Enum
from numpy import sum as npsum

class ECity(Enum):
    Saint_Petersburg = 0,
    Moscow = 1,
    Novosibirsk = 2,
    Yekaterinburg = 3,
    Kazan = 4,
    Nizhny_Novgorod = 5,
    Chelyabinsk = 6,
    Omsk = 7,
    Samara = 8,
    Rosrov_on_Don = 9,
    Ufa = 10,
    Krasnoyarsk = 11,
    Perm = 12,
    Voronezh = 13,
    Volgograd = 14,
    Kaliningrad = 15


class B_box:
    def __init__(self, _lat_min: float = 0, _lat_max: float = 0, _lon_min: float = 0, _lon_max: float = 0) -> None:
        self.lat_max = _lat_max
        self.lat_min = _lat_min
        self.lon_max = _lon_max
        self.lon_min = _lon_min

class Center:
    def __init__(self, _lat: float = 0, _lon: float = 0) -> None:
        self.lat = _lat
        self.lon = _lon

class City:
    name: str = '',
    population: int = 0
    b_box: B_box = B_box()
    center: Center = Center()

    def __init__(self, _name: str, _population: int, _lat: float, _lon: float,
                 _lat_min: float, _lat_max: float, _lon_min: float, _lon_max: float) -> None:
        self.name = _name
        self.population = _population
        self.b_box = B_box(_lat_min, _lat_max, _lon_min, _lon_max)
        self.center = Center(_lat, _lon)


OverpassCityMapping = {
    ECity.Saint_Petersburg: City('Санкт-Петербург', 5_601_911, 59.95001, 30.31661, 59.63,  60.25, 29.41, 30.77),
    ECity.Moscow: City('Москва', 13_010_112, 55.75583, 37.61778, 55.14,  56.02, 36.79, 37.97),
    ECity.Novosibirsk: City('Новосибирск', 1_633_595, 55.01666, 82.91665, 54.79, 55.15, 82.73, 83.16),
    ECity.Yekaterinburg: City('Екатеринбург', 1_544_376, 56.83333, 60.58327, 56.65, 56.97, 60.37, 60.86),
    ECity.Kazan: City('Казань', 1_308_660, 55.79083, 49.11444, 55.67,  55.92, 48.83, 49.32),
    ECity.Nizhny_Novgorod: City('Нижний Новгород', 1_249_861, 56.32694, 44.0075, 56.18, 56.40, 43.71,  44.14),
    ECity.Chelyabinsk: City('Челябинск', 1_189_525, 55.16222, 61.40306, 54.98, 55.32, 61.22, 61.60),
    ECity.Omsk: City('Омск', 1_125_695, 54.96681, 73.38321, 54.82, 55.15, 73.09, 73.61),
    ECity.Samara: City('Самара', 1_173_299, 53.18334, 50.11671, 53.09, 53.44, 50.00, 50.39),
    ECity.Rosrov_on_Don: City('Ростов-на-Дону', 1_142_162, 47.24056, 39.71056, 47.15, 47.37, 39.40, 39.86),
    ECity.Ufa: City('Уфа', 1_144_809, 54.73347, 55.96659, 54.52, 54.96, 55.76, 56.26),
    ECity.Krasnoyarsk: City('Красноярск', 1_187_771, 56.01194, 92.87139, 55.92, 56.14, 92.61, 93.12),
    ECity.Perm: City('Пермь', 1_034_002, 58.01389, 56.24889, 57.86, 58.18, 55.80, 56.66),
    ECity.Voronezh: City('Воронеж', 1_057_681, 51.67167, 39.21056, 51.50, 51.82, 39.01, 39.41),
    ECity.Volgograd: City('Волгоград', 1_028_036, 48.71167, 44.51389, 48.45, 48.89, 44.32, 44.69),
    ECity.Kaliningrad: City('Калининград', 490_449, 54.71666, 20.49991, 54.64, 54.78, 20.29, 20.64)
}


def get_total_population() -> int:
    return npsum([OverpassCityMapping[city].population for city in OverpassCityMapping])
