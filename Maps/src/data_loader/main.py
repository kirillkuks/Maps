from overpass import Overpass


def main():
    maps = Overpass()
    maps.request()


if __name__ == '__main__':
    main()
