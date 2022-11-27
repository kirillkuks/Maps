import json
import copy

class OSMJsonDataProcessor:
    _saved = {
        'top' : ['elements'],
        'elements' : ['lat', 'lon', 'tags', 'center'],
        'tags' : [
            'addr:city'
            'addr:housenumber',
            'addr:street',
            'amenity',
            'building',
            'name',
            'website',
            'contact:website',
            'shop',
            'phone',
            'opening_hours',
            'toilets',
            'colour',
            'station',
            'network',
            'tourism',
            'public_transport',
            'sport',
            'office',
            'water'
            ]
    }

    _arrays = ['top', 'elements']
    _singles = ['tags']
    _extract = ['center']

    def __init__(self, json_data) -> None:
        self._json_data = copy.deepcopy(json_data)

    def clear_json_from_file(self, filename, saved_fields):
        with open(filename, mode='r', encoding='utf-8') as f:
            return self.clear_json(json_data=json.load(f), saved_fields=saved_fields)

    def clear_json(self):
        res = self._clear_json(self._json_data, 'top')
        return res['elements']

    def _clear_json(self, json_data, field_name):
        saved = self._saved[field_name]

        to_delete = []
        to_add = {}

        for name in json_data:
            if name not in saved:
                to_delete.append(name)

            else:
                if name in self._arrays:
                    for i, sub_json in enumerate(json_data[name]):
                        json_data[name][i] = self._clear_json(sub_json, name)

                elif name in self._singles:
                    json_data[name] = self._clear_json(json_data[name], name)

                elif name in self._extract:
                    for field in json_data[name]:
                        to_add[field] = json_data[name][field]

                    to_delete.append(name)

        for for_deletion in to_delete:
            json_data.pop(for_deletion)

        for for_add_keys in to_add.keys():
            json_data[for_add_keys] = to_add[for_add_keys]

        return json_data
