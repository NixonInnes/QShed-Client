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


class GatewayModule:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms
        self.logger = logging.getLogger(self.__class__.__name__)

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


# class NoSQLCollection:
#     def __init__(self, comms: Comms, database_name: str, collection_name: str) -> None:
#         self.comms = comms
#         self.database_name = database_name
#         self.name = collection_name
#         self.logger = logging.getLogger(self.__class__.__name__)

#     @typed_response(response_model=responseModels.JSONResponse)
#     def get(self, limit: int = 10, query: Optional[Dict[str, str]] = {}) -> Union[Dict, List]:
#         return self.comms.get(f"nosql/{self.database_name}/{self.name}/get/{limit}")

#     @property
#     def data(self) -> Union[Dict, List]:
#         return self.get()

#     def dataframe(self, limit: int = 10, **params) -> pd.DataFrame:
#         data = self.get(limit=limit, **params)
#         return pd.DataFrame([flatten_dict(item) for item in data])

#     def insert(self, data: Union[list, dict]) -> Union[str, List[str]]:
#         if isinstance(data, list):
#             return self.insert_many(data)
#         else:
#             return self.insert_one(data)

#     @typed_response(response_model=responseModels.StrResponse)
#     def insert_one(self, data: dict) -> str:
#         return self.comms.post(
#             f"nosql/{self.database_name}/{self.name}/insert", data=data
#         )

#     @typed_response(response_model=responseModels.StrListResponse)
#     def insert_many(self, data: list) -> List[str]:
#         return self.comms.post(
#             f"nosql/{self.database_name}/{self.name}/insert/many", data=data
#         )

#     @typed_response(response_model=responseModels.BoolResponse)
#     def delete_one(self, key: str, query: str) -> None:
#         return self.comms.post(
#             f"nosql/{self.database_name}/{self.name}/delete/one",
#             data={"key":key, "query": query},
#         )

#     @typed_response(response_model=responseModels.IntResponse)
#     def delete_many(self, key: str, query: str) -> int:
#         return self.comms.post(
#             f"nosql/{self.database_name}/{self.name}/delete/many",
#             data={"key":key, "query": query},
#         )

#     @typed_response(response_model=responseModels.IntResponse)
#     def delete_all(self, confirm: bool = False) -> int:
#         return self.comms.post(
#             f"nosql/{self.database_name}/{self.name}/delete/all",
#             params={"confirm": confirm},
#         )


# class NoSQLDatabase:
#     def __init__(self, comms: Comms, database_name: str) -> None:
#         self.comms = comms
#         self.name = database_name
#         self.logger = logging.getLogger(self.__class__.__name__)

#     def __getitem__(self, key: str) -> NoSQLCollection:
#         return NoSQLCollection(self.comms, self.name, key)

#     @typed_response(response_model=responseModels.StrListResponse)
#     def list(self) -> List[str]:
#         return self.comms.get(f"nosql/{self.name}/list")


# class NoSQL:
#     def __init__(self, comms: Comms) -> None:
#         self.comms = comms

#     def __getitem__(self, key: str) -> NoSQLDatabase:
#         return NoSQLDatabase(self.comms, key)

#     @typed_response(response_model=responseModels.StrListResponse)
#     def list(self) -> List[str]:
#         return self.comms.get("nosql/list")

#     @typed_response(response_model=responseModels.StrResponse)
#     def create(self, database_name: str) -> str:
#         return self.comms.post(
#             "nosql/create", params={"database_name": database_name}
#         )


class EntityModule:
    def __init__(self, comms: Comms):
        self.comms = comms
        self.logger = logging.getLogger(self.__class__.__name__)

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



class TimeseriesModule:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms
        self.logger = logging.getLogger(self.__class__.__name__)

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


#     @typed_response(response_model=responseModels.TimeseriesResponse)
#     def get(
#         self,
#         name: Union[str, int, dataModels.TimeseriesRecord],
#         start: Optional[datetime] = None,
#         end: Optional[datetime] = None,
#     ) -> pd.DataFrame:
#         if end is None:
#             end = datetime.utcnow()
#         if start is None:
#             start = end - timedelta(days=365)
        
#         params = dict(
#             start=start.strftime("%Y/%m/%d %H:%M:%S"),
#             end=end.strftime("%Y/%m/%d %H:%M:%S")
#         )

#         if isinstance(name, dataModels.TimeseriesRecord):
#             name = name.id
            
#         if type(name) is int:
#             return self.comms.get(f"timeseries/get/id/{name}", params=params)
#         return self.comms.get(f"timeseries/get/{name}", params=params)

#     @typed_response(response_model=responseModels.TimeseriesRecordResponse)
#     def create(self, name):
#         record = dataModels.TimeseriesRecord(name=name)
#         return self.comms.post("timeseries/create", data=record.dict())

#     @typed_response(response_model=responseModels.TimeseriesResponse)
#     def set(
#         self,
#         name: Union[str, int],
#         df: pd.DataFrame
#     ):
#         if type(name) is int:
#             return self.comms.post(f"timeseries/set/id/{name}", data={"json_str": df.to_json()})
#         return self.comms.post(f"timeseries/set/{name}", data={"json_str": df.to_json()})

#     @typed_response(response_model=responseModels.TimeseriesRecordListResponse)
#     def list(self):
#         return self.comms.get("timeseries/list")

#     @typed_response(response_model=responseModels.TimeseriesRecordListResponse)
#     def scan(self):
#         return self.comms.post("timeseries/scan")



class QShedClient:
    def __init__(self, gateway_address: str, config_file: str = "") -> None:
        self.comms = Comms(gateway_address)
        self.gateway = GatewayModule(self.comms)
        #self.nosql = NoSQL(self.comms)
        #self.scheduler = Scheduler(self.comms)
        self.entity = EntityModule(self.comms)
        self.ts = TimeseriesModule(self.comms)

