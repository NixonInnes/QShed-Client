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
        seconds=config["caching"]["lifetime"], 
        maxsize=config["caching"]["maxsize"]
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


class Module:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms
        self.logger = logging.getLogger(self.__class__.__name__)


class Gateway(Module):
    def ping(self):
        return self.comms.get(f"ping")


# class Scheduler:
#     def __init__(self, comms: Comms) -> None:
#         self.comms = comms
#         self.logger = logging.getLogger(self.__class__.__name__)

#     @typed_response(response_model=responseModels.StrResponse)
#     def add(
#         self,
#         url: str,
#         interval: int,
#         method: str = "get",
#         params: Dict[str, str] = {},
#         data: Dict[str, Optional[str]] = {},
#         headers: Dict[str, str] = {},
#     ) -> str:
#         request = dataModels.Request(
#             method=method, url=url, params=params, data=data, headers=headers
#         )
#         schedule = dataModels.Schedule(interval=interval, request=request)
#         return self.comms.post(f"scheduler/add", data=schedule.json(exclude_none=True))

#     @typed_response(response_model=responseModels.SchedulesResponse)
#     def list(self) -> List[dataModels.Schedule]:
#         return self.comms.get("scheduler/list")



class EntityModule(BaseModule):
    @typed_response(response_model=responseModels.EntityListResponse)
    def get(self, *ids: List[int]):
        return self.comms.get(f"entity/get", params={"id": ids})

    @typed_response(response_model=responseModels.EntityListResponse)
    def get_roots(self):
        return self.comms.get("entity/get_roots")

    @typed_response(response_model=responseModels.EntityResponse)
    def create(self, entity: dataModels.Entity):
        return self.comms.post("entity/create", data=entity.json())

    @typed_response(response_model=responseModels.EntityListResponse)
    def create_many(self, *entities: List[dataModels.Entity]):
        return self.comms.post(
            "entity/create_many", 
            data=[entity.dict() for entity in entities]
        )



class TimeseriesModule(BaseModule):
    @typed_response(response_model=responseModels.TimeseriesListResponse)
    def get(
        self, 
        *ids: List[int], 
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ):
        if end is None:
            end = datetime.utcnow()
        if start is None:
            start = end - timedelta(days=365)

        params = dict(
            start=start.timestamp(),
            end=end.timestamp(),
            id=ids
        )
        return self.comms.get("timeseries/get", params=params)

    @typed_response(response_model=responseModels.TimeseriesResponse)
    def create(self, timeseries: dataModels.Timeseries):
        return self.comms.post("timeseries/create", data=timeseries.json())

    @typed_response(response_model=responseModels.TimeseriesResponse)
    def add(self, timeseries: dataModels.Timeseries):
        return self.comms.post("timeseries/add", data=timeseries.json())

#     @typed_response(response_model=responseModels.TimeseriesRecordListResponse)
#     def list(self):
#         return self.comms.get("timeseries/list")

#     @typed_response(response_model=responseModels.TimeseriesRecordListResponse)
#     def scan(self):
#         return self.comms.post("timeseries/scan")


class CollectionModule(BaseModule):
    @typed_response(response_model=responseModels.CollectionListResponse)
    def get(self, *ids: List[int]):
        return self.comms.get(f"collection/get", params={"id": ids})




class QShedClient:
    def __init__(self, gateway_address: str, config_file: str = "") -> None:
        self.comms = Comms(gateway_address)
        self.gateway = GatewayModule(self.comms)
        self.entity = EntityModule(self.comms)
        self.timeseries = TimeseriesModule(self.comms)
        self.collection = CollectionModule(self.comms)

    @property
    def ts(self):
        return self.timeseries

    @property
    def col(self):
        return self.collection

