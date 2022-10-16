from overpass import Overpass
from data_processor import OSMJsonDataProcessor

import json


def main():
    maps = Overpass()
    data = maps.request()

    processor = OSMJsonDataProcessor(data)
    data = processor.clear_json()

    with open('res.json', mode='w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)



if __name__ == '__main__':
    main()
