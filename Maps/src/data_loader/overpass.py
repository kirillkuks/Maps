import requests
from map_features import MapObjects, QueryMapping
from cities import ECity, OverpassCityMapping

class Overpass:
    def __init__(self) -> None:
        self._overpass_url = 'http://overpass-api.de/api/interpreter'

    def request(self, city : ECity, map_object : OverpassCityMapping):
        overpass_query = self._query(city=OverpassCityMapping[city].name, tag=QueryMapping[map_object])
        response = requests.get(self._overpass_url, 
                        params={'data': overpass_query})

        data = {}
        response.encoding = 'utf-8'

        print(f'{map_object} : {response.status_code}')

        if response.status_code != 204:
            data = response.json()

        return data

    def requestAll(self, city : ECity):
        for map_object in MapObjects:
            yield self.request(city=city, map_object=map_object)

    def _query(self, city : str, tag : str) -> str:
        return  f'''
            [out:json];
            area[name="{city}"];
            nwr[{tag}](area);
            out center;
        '''
