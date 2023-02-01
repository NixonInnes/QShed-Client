import logging
from typing import List, Dict, Optional, Union
import pandas as pd
import requests
import json
from pydantic import parse_obj_as
from datetime import datetime, timedelta

from . import config
from .models import data as dataModels
from .models import response as responseModels
from .utils import typed_response, flatten_dict, timed_lru_cache


class Comms:
    def __init__(self, address: str) -> None:
        if not address.endswith("/"):
            address += "/"
        self.address = address
        self.headers = {"Content-Type": "application/json"}
        self.logger = logging.getLogger(self.__class__.__name__)

    @timed_lru_cache(
        seconds=config["caching"]["lifetime"], maxsize=config["caching"]["maxsize"]
    )
    def cached_get(self, address):
        self.logger.debug("Returning cached response")
        return requests.get(address)

    def getter(self, address, params={}):
        if config["caching"]["enabled"]:
            if not params:
                try:
                    return self.cached_get(address)
                except TypeError as t:
                    pass
        return requests.get(address, params=params)

    def poster(self, address, data="", params={}, headers={}):
        return requests.post(address, data=data, params=params, headers=headers)

    def get(self, url_ext: str, params: dict = {}):
        resp = self.getter(self.address + url_ext, params=params)
        self.logger.debug(f"STATUS: {resp.status_code} CONTENT: {resp.content}")

        if resp.ok:
            return resp.text
        else:
            raise Exception(f"Error {resp.status_code}: {resp.text}")

    def post(self, url_ext: str, params: dict = {}, data: str = ""):
        if not isinstance(data, str):
            data = json.dumps(data)
        resp = self.poster(
            self.address + url_ext, data=data, params=params, headers=self.headers
        )
        self.logger.debug(f"STATUS: {resp.status_code} CONTENT: {resp.content}")

        if resp.ok:
            return resp.text
        else:
            raise Exception(f"Error {resp.status_code}: {resp.content}")


class BaseModule:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms
        self.logger = logging.getLogger(self.__class__.__name__)


class GatewayModule(BaseModule):
    def ping(self):
        return self.comms.get(f"ping")


class EntityModule(BaseModule):
    @typed_response
    def get(self, *ids: List[int]) -> responseModels.EntityListResponse:
        return self.comms.get(f"entity/get", params={"id": ids})

    @typed_response
    def get_roots(self) -> responseModels.EntityListResponse:
        return self.comms.get("entity/get_roots")

    @typed_response
    def create(self, entity: dataModels.Entity) -> responseModels.EntityResponse:
        return self.comms.post("entity/create", data=entity.json())

    @typed_response
    def create_many(
        self, *entities: List[dataModels.Entity]
    ) -> responseModels.EntityListResponse:
        return self.comms.post(
            "entity/create_many", data=[entity.dict() for entity in entities]
        )


class TimeseriesModule(BaseModule):
    @typed_response
    def get(
        self,
        *ids: List[int],
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> responseModels.TimeseriesListResponse:
        if end is None:
            end = datetime.utcnow()
        if start is None:
            start = end - timedelta(days=365)

        params = dict(start=start.timestamp(), end=end.timestamp(), id=ids)
        return self.comms.get("timeseries/get", params=params)

    @typed_response
    def create(
        self, timeseries: dataModels.Timeseries
    ) -> responseModels.TimeseriesResponse:
        return self.comms.post("timeseries/create", data=timeseries.json())

    @typed_response
    def add(
        self, timeseries: dataModels.Timeseries
    ) -> responseModels.TimeseriesResponse:
        return self.comms.post("timeseries/add", data=timeseries.json())


class CollectionModule(BaseModule):
    @typed_response
    def get(
        self, *ids: List[int], limit: int = 10, query: Optional[Dict] = None
    ) -> responseModels.CollectionListResponse:
        params = {
            "id": ids,
            "limit": limit,
        }
        if query is not None:
            params["query"] = query

        return self.comms.get(f"collection/get", params=params)

    @typed_response
    def create(
        self, collection: dataModels.Collection
    ) -> responseModels.CollectionResponse:
        return self.comms.post("collection/create", data=collection.json())

    @typed_response
    def get_database(
        self, *ids: List[int]
    ) -> responseModels.CollectionDatabaseListResponse:
        return self.comms.get("collection/database/get", params={"id": ids})

    @typed_response
    def create_database(
        self, collection_db: dataModels.CollectionDatabase
    ) -> responseModels.CollectionDatabaseResponse:
        return self.comms.post("collection/database/create", data=collection_db.json())


class DataModelModule(BaseModule):
    def get_definition(self, name: str):
        return self.comms.get(f"datamodel/{name}/definition")

    def save_definition(self, name: str, datamodel: dataModels.DataModel):
        return self.comms.post(
            f"datamodel/{name}/definition", data=datamodel.get_definition()
        )


class QShedClient:
    def __init__(self, gateway_address: str, config_file: str = "") -> None:
        self.comms = Comms(gateway_address)
        self.gateway = GatewayModule(self.comms)
        self.entity = EntityModule(self.comms)
        self.timeseries = TimeseriesModule(self.comms)
        self.collection = CollectionModule(self.comms)
        self.datamodel = DataModelModule(self.comms)

    @property
    def ts(self):
        return self.timeseries

    @property
    def col(self):
        return self.collection
