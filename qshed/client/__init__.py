import yaml

with open("config.yml", "r") as config_file:
    config = yaml.safe_load(config_file)

from .client import QShedClient
