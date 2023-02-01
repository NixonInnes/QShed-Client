import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level="DEBUG",
    format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

env_file = ".env"
if os.path.exists(env_file):
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file)
        logger.info(f"Loaded environment file: {env_file}")
    except Exception as e:
        logger.warn(f"{e} - Unable to load environment file: {env_file}")

logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import yaml

with open("config.yml", "r") as config_file:
    config = yaml.safe_load(config_file)

from .client import QShedClient
