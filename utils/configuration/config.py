import json
import logging

__all__ = ['Configuration']


class Configuration(object):
    def __init__(self):
        try:
            with open('../config.json', 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            logging.error('Config file is not found')
            raise
