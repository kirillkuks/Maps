from enum import Enum

class City(Enum):
    Saint_Petersburg = 0,
    Moscow = 1


OverpassCityMapping = {
    City.Saint_Petersburg : 'Санкт-Петербург',
    City.Moscow : 'Москва'
}
