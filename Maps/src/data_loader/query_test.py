from overpass import Overpass, MapObjects
from cities import ECity, OverpassCityMapping
from data_processor import OSMJsonDataProcessor

import json


def main():
    maps = Overpass()

    city = ECity.Saint_Petersburg

    with open(f'test/{city.name}.json', mode='w', encoding='utf8') as f:
        f.write('[\n {  }')

        data = maps.request(city, MapObjects.BOUNDARY)
        processor = OSMJsonDataProcessor(data)
        #postprocessed = processor.clear_json()

        for record in data:
            f.write('\n,\n')
            json.dump(record, f, ensure_ascii=False, indent=4)

        f.write('\n]')

    print(f'{city.name} ({OverpassCityMapping[city].name}) is finished')


if __name__ == '__main__':
    main()
