import argparse
import os
import json
import neuronsim


def main():
    parser = argparse.ArgumentParser(description='Generate remote simulation location data.')
    parser.add_argument("config_file", nargs='?', default="geo_config.json", type=str,
                        help="configuration file of geo location data")
    args = parser.parse_args()
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    config_file = os.path.join(__location__, args.config_file)
    geo_info_test = neuronsim.GeoInfo(config_file)
    geo_info_test.print_remotes_datafiles()
    location_csv = 'location_data'
    geo_info_test.parse_csv(location_csv)
    geo_info_test.insert_csv(location_csv)
    # geo_info_test.parse_insert()


if __name__ == "__main__":
    main()
