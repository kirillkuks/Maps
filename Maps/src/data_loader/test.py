from overpass import Overpass, MapObjects, City
from data_processor import OSMJsonDataProcessor

import json


def main():
    maps = Overpass()

    with open('cities/Moscow.json', mode='w', encoding='utf8') as f:
        f.write('[\n {  }')

        for data in maps.requestAll(city=City.Moscow):
            processor = OSMJsonDataProcessor(data)
            postprocessed = processor.clear_json()

            for record in postprocessed:
                f.write('\n,\n')
                json.dump(record, f, ensure_ascii=False, indent=4)

        f.write('\n]')


if __name__ == '__main__':
    main()
