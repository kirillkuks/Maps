import json
import requests


class Overpass:
    def __init__(self) -> None:
        self._overpass_url = "http://overpass-api.de/api/interpreter"

    def request(self) -> None:
        overpass_query = """
            [out:json];
            area[name="Санкт-Петербург"];
            nwr[amenity=university](area);
            out center;
        """

        response = requests.get(self._overpass_url, 
                        params={'data': overpass_query})

        response.encoding = "utf-8"
        data = response.json()

        with open("res.json", mode="w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
