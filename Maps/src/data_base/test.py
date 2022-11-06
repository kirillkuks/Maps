from maps_database import MapsDB

import json


def main():
    client = MapsDB()
    data = None

    for city in ['SPB', 'Moscow']:
        with open(f'cities/{city}.json', mode='r', encoding='utf8') as f:
            data = json.load(f)

        client.create_collection(city)
        client.add_json(json_data=data, collection_name=city)

        col = client.get_collection(city)
        res = col.find({'center.lat' : {'$gt' : 60.0}})

        for r in res:
            print(r)


if __name__ == '__main__':
    main()
