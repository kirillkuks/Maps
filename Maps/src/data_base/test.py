import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
print(abs_path[:abs_path.rindex('\\')] + '\\data_loader')
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')

from cities import OverpassCityMapping

from maps_database import MapsDB


import json


def main():
    client = MapsDB()
    data = None

    for city in OverpassCityMapping:
        client.get_collection(city.name).delete_many({})

        with open(f'cities/{city.name}.json', mode='r', encoding='utf8') as f:
            data = json.load(f)

        client.create_collection(city.name)
        client.add_json(json_data=data, collection_name=city.name)

if __name__ == '__main__':
    main()
