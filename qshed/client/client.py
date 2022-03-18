from typing import List, Dict, Optional, Union
import pandas as pd
import requests
import json
from pydantic import parse_obj_as
from datetime import datetime

from .decorators import typed_response
from .models import data as dataModels, response as responseModels
from .utils import flatten_dict


class Comms:
    def __init__(self, address: str) -> None:
        if not address.endswith("/"):
            address += "/"
        self.address = address
        self.headers = {"Content-Type": "application/json"}

    def get(self, url_ext: str, params: dict = {}):
        resp = requests.get(self.address + url_ext, params=params)
        if resp.ok:
            return resp.text
        else:
            raise Exception(f"Error {resp.status_code}: {resp.content}")

    def post(self, url_ext: str, params: dict = {}, data: dict = {}):
        if not isinstance(data, str):
            data = json.dumps(data)
        resp = requests.post(
            self.address + url_ext, data=data, params=params, headers=self.headers
        )
        if resp.ok:
            return resp.text
        else:
            raise Exception(f"Error {resp.status_code}: {resp.content}")


class Scheduler:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms

    @typed_response(response_model=responseModels.StrResponse)
    def add(
        self,
        url: str,
        interval: int,
        method: str = "get",
        params: Dict[str, str] = {},
        data: Dict[str, Optional[str]] = {},
        headers: Dict[str, str] = {},
    ) -> str:
        request = dataModels.Request(
            method=method, url=url, params=params, data=data, headers=headers
        )
        schedule = dataModels.Schedule(interval=interval, request=request)
        return self.comms.post(f"scheduler/add", data=schedule.json(exclude_none=True))

    @typed_response(response_model=responseModels.SchedulesResponse)
    def list(self) -> List[dataModels.Schedule]:
        return self.comms.get("scheduler/list")


class NoSQL:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms

    def __getitem__(self, key: str) -> NoSQLDatabase:
        return NoSQLDatabase(self.comms, key)

    @typed_response(response_model=responseModels.ListResponse)
    def list(self) -> List[str]:
        return self.comms.get("database/list")

    @typed_response(response_model=responseModels.StrResponse)
    def create(self, database_name: str) -> str:
        return self.comms.post(
            "database/create", params={"database_name": database_name}
        )


class NoSQLDatabase:
    def __init__(self, comms: Comms, database_name: str) -> None:
        self.comms = comms
        self.name = database_name

    def __getitem__(self, key: str) -> NoSQLCollection:
        return NoSQLCollection(self.comms, self.name, key)

    @typed_response(response_model=responseModels.ListResponse)
    def list(self) -> List[str]:
        return self.comms.get(f"database/{self.name}/list")


class NoSQLCollection:
    def __init__(self, comms: Comms, database_name: str, collection_name: str) -> None:
        self.comms = comms
        self.database_name = database_name
        self.name = collection_name

    @typed_response(response_model=responseModels.JSONResponse)
    def get(self, limit: int = 10, query: Optional[Dict[str, str]] = {}) -> Union[Dict, List]:
        return self.comms.get(f"database/{self.database_name}/{self.name}/get/{limit}")

    @property
    def data(self) -> Union[Dict, List]:
        return self.get()

    def dataframe(self, limit: int = 10, **params) -> pd.DataFrame:
        data = self.get(limit=limit, **params)
        return pd.DataFrame([flatten_dict(item) for item in data])

    def insert(self, data: Union[list, dict]) -> Union[str, List[str]]:
        if isinstance(data, list):
            return self.insert_many(data)
        else:
            return self.insert_one(data)

    @typed_response(response_model=responseModels.StrResponse)
    def insert_one(self, data: dict) -> str:
        return self.comms.post(
            f"database/{self.database_name}/{self.name}/insert", data=data
        )

    @typed_response(response_model=responseModels.ListResponse)
    def insert_many(self, data: list) -> List[str]:
        return self.comms.post(
            f"database/{self.database_name}/{self.name}/insert/many", data=data
        )

    @typed_response(response_model=responseModels.Response)
    def delete_one(self, query: str) -> None:
        return self.comms.post(
            f"database/{self.database_name}/{self.name}/delete/one",
            data={"query": query},
        )

    @typed_response(response_model=responseModels.IntResponse)
    def delete_many(self, query: str) -> int:
        return self.comms.post(
            f"database/{self.database_name}/{self.name}/delete/many",
            data={"query": query},
        )

    @typed_response(response_model=responseModels.IntResponse)
    def delete_all(self, confirm: bool = False) -> int:
        return self.comms.post(
            f"database/{self.database_name}/{self.name}/delete/all",
            params={"confirm": confirm},
        )


class Timeseries:
    def __init__(self, comms: Comms) -> None:
        self.comms = comms

    @typed_response(response_model=responseModels.TagResponse)
    def get(
        self,
        tag_names: Union[str, List[str]],
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pd.DataFrame:
        params = {}
        if start:
            params["start"] = datetime.strftime(start, "%y/%m/%d %H:%M:%S")
        if end:
            params["end"] = datetime.strftime(end, "%y/%m/%d %H:%M:%S")
        return self.comms.get(f"timeseries/get/{tag_name}", params=params)


class QShedClient:
    def __init__(self, gateway_address: str) -> None:
        self.comms = Comms(gateway_address)
        self.database = NoSQLDatabase(self.comms)
        self.scheduler = Scheduler(self.comms)
        self.ts = Timeseries(self.comms)
