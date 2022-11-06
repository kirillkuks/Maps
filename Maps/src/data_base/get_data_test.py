from maps_database import MapsDB

def main():
    client = MapsDB()

    col = client.get_collection('SPB')
    res = col.find({'center.lat' : {'$gt' : 60.0}})

    for r in res:
        print(r)



if __name__ == '__main__':
    main()
