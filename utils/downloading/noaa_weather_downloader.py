import pandas as pd
import logging
import requests
import json
import time

__all__ = ['NOAAWeatherDownloader']


class NOAAWeatherDownloader(object):
    def __init__(self, api_key, weather_code_dict_file):
        self.api_key = api_key
        try:
            self.weather_code_dict = pd.read_csv(weather_code_dict_file)
        except FileNotFoundError:
            logging.error('Could not open dict file')
            raise

    @property
    def __request_url(self):
        return 'http://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid={0}&startdate={' \
                  '1}&enddate={2}&limit={3}&offset={4}'

    def get_city_id_by_name(self, city_name, country_code):
        try:
            return self.weather_code_dict[(self.weather_code_dict['country'] == country_code)
                                          & (self.weather_code_dict['name'] == city_name)]['id'].item()
        except ValueError:
            logging.error('Could not find city with the specified name: {}'.format(city_name))

    def get_weather_for_city_by_name(self, city_name, country_code, start_date, end_date, limit=1000, offset=0):
        city_id = self.get_city_id_by_name(city_name, country_code)
        return self.get_weather_for_city_by_id(city_id, start_date, end_date, limit, offset)

    def get_weather_for_city_by_id(self, city_id, start_date, end_date, limit=1000, offset=0):
        req_url = self.__request_url.format(city_id, start_date, end_date, limit, offset)

        time.sleep(2)

        result_json = requests.get(req_url,
                                   headers={'token': self.api_key}, timeout=50)

        result_json = json.loads(result_json.content)
        result_data = pd.DataFrame(result_json['results'])

        if result_json['metadata']['resultset']['count'] > offset + limit:
            return pd.concat(
                [result_data, self.get_weather_for_city_by_id(city_id, start_date, end_date, limit, offset + limit)])
        else:
            return result_data





