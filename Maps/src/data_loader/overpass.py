import requests


class Overpass:
    def __init__(self) -> None:
        self._overpass_url = 'http://overpass-api.de/api/interpreter'

    def request(self):
        overpass_query = '''
            [out:json];
            area[name="Санкт-Петербург"];
            nwr[amenity=university](area);
            out center;
        '''

        response = requests.get(self._overpass_url, 
                        params={'data': overpass_query})

        response.encoding = 'utf-8'
        data = response.json()

        return data
