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


class City:
    class Center:
        def __init__(self, _lat : float = 0, _lon : float = 0) -> None:
            self.lat = _lat
            self.lon = _lon

    name : str = '',
    population : int = 0
    center : Center = Center()

    def __init__(self, _name : str, _population : int, _lat : float, _lon : float) -> None:
        self.name = _name
        self.population = _population
        self.center = City.Center(_lat, _lon)


OverpassCityMapping = {
    ECity.Saint_Petersburg : City('Санкт-Петербург', 5_601_911, 59.95001, 30.31661),
    ECity.Moscow : City('Москва', 13_010_112, 55.75583, 37.61778),
    ECity.Novosibirsk : City('Новосибирск', 1_633_595, 55.01666, 82.91665),
    ECity.Yekaterinburg : City('Екатеринбург', 1_544_376, 56.83333, 60.58327),
    ECity.Kazan : City('Казань', 1_308_660, 55.79083, 49.11444),
    ECity.Nizhny_Novgorod : City('Нижний Новгород', 1_249_861, 56.32694, 44.0075),
    ECity.Chelyabinsk : City('Челябинск', 1_189_525, 55.16222, 61.40306),
    ECity.Omsk : City('Омск', 1_125_695, 54.96681, 73.38321),
    ECity.Samara : City('Самара', 1_173_299, 53.18334, 50.11671),
    ECity.Rosrov_on_Don : City('Ростов-на-Дону', 1_142_162, 47.24056, 39.71056),
    ECity.Ufa : City('Уфа', 1_144_809, 54.73347, 55.96659),
    ECity.Krasnoyarsk : City('Красноярск', 1_187_771, 56.01194, 92.87139),
    ECity.Perm : City('Пермь', 1_034_002, 58.01389, 56.24889),
    ECity.Voronezh : City('Воронеж', 1_057_681, 51.67167, 39.21056),
    ECity.Volgograd : City('Волгоград', 1_028_036, 48.71167, 44.51389),
    ECity.Kaliningrad : City('Калининград', 490_449, 54.71666, 20.49991)
}

def get_total_population() -> int:
    return npsum([OverpassCityMapping[city].population for city in OverpassCityMapping])
