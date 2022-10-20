import json
import copy

class OSMJsonDataProcessor:
    _saved = {
        'top' : ['elements'],
        'elements' : ['id', 'lat', 'lon', 'tags', 'center'],
        'tags' : ['addr:housenumber', 'addr:street', 'amenity', 'name', 'website']
    }

    _arrays = ['top', 'elements']
    _singles = ['tags']

    def __init__(self, json_data) -> None:
        self._json_data = copy.deepcopy(json_data)

    def clear_json_from_file(self, filename, saved_fields):
        with open(filename, mode='r', encoding='utf-8') as f:
            return self.clear_json(json_data=json.load(f), saved_fields=saved_fields)

    def clear_json(self):
        return self._clear_json(self._json_data, 'top')        

    def _clear_json(self, json_data, field_name):
        saved = self._saved[field_name]

        to_delete = []

        for name in json_data:
            if name not in saved:
                to_delete.append(name)

            else:
                if name in self._arrays:
                    for i, sub_json in enumerate(json_data[name]):
                        json_data[name][i] = self._clear_json(sub_json, name)

                elif name in self._singles:
                    json_data[name] = self._clear_json(json_data[name], name)

        for for_deletion in to_delete:
            json_data.pop(for_deletion)

        return json_data
