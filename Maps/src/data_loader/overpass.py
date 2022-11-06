import requests
from map_features import MapObjects, QueryMapping
from cities import City, OverpassCityMapping


class Overpass:
    def __init__(self) -> None:
        self._overpass_url = 'http://overpass-api.de/api/interpreter'

    def request(self, city, map_object):
        overpass_query = self._query(city=OverpassCityMapping[city], tag=QueryMapping[map_object])
        response = requests.get(self._overpass_url, 
                        params={'data': overpass_query})

        response.encoding = 'utf-8'
        data = response.json()

        return data

    def requestAll(self, city):
        for map_object in MapObjects:
            yield self.request(city=city, map_object=map_object)


    def _query(self, city, tag) -> str:
        return  f'''
            [out:json];
            area[name="{city}"];
            nwr[{tag}](area);
            out center;
        '''
