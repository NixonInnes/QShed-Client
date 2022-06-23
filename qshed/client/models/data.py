import json
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List, Any
from pydantic import BaseModel, Field, validator
from pydantic.dataclasses import dataclass

from ..utils import string_hash, zip_str, unzip_str



class QShedModel(BaseModel):
    _is_error = False

    def yaml(self) -> str:
        return yaml.dump(self.dict())


class QShedModelArb(QShedModel):
    class Config:
        arbitrary_types_allowed = True


# class Request(QShedModel):
#     url: str
#     method: str
#     params: Dict[str, str] = Field({})
#     data: Dict[str, str] = Field({})
#     headers: Dict[str, str] = Field({})

#     @validator("method")
#     def check_valid_method(cls, method: str) -> str:
#         if method.lower() not in ("get", "post"):
#             raise ValueError("must be either 'get' or 'post'")
#         return method.lower()

#     def get_collection_id(self) -> str:
#         return string_hash(self.json())


# class Schedule(QShedModel):
#     request: Request
#     interval: int
#     id: Optional[str]
#     name: Optional[str]
#     next_run: Optional[float]

#     def get_collection_id(self) -> str:
#         return self.request.get_collection_id()


class Entity(QShedModel):
    id: Optional[int] = None
    name: str
    display_name: Optional[str]
    data_: str
    type: int
    parent: Optional[int]
    children: Optional[List[int]] = Field([])
    timeseries: Optional[List[int]] = Field([])
    collections: Optional[List[int]] = Field([])

    __types = {
        0: str,
        1: int,
        2: float,
        3: json.loads
    }

    @property
    def data(self):
        return self.__types[self.type](self.data_)

    def get_types(self):
        return self.__types


def ts_json_loads(v):
    dic = json.loads(v)
    dic["data"] = pd.read_json(unzip_str(dic["data"]))
    return dic


class Timeseries(QShedModel):
    name: str
    data: pd.DataFrame
    entity: Optional[int]
    id: Optional[int] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            pd.DataFrame: lambda df: zip_str(df.to_json())
        }
        json_loads = ts_json_loads


class CollectionDatabase(QShedModel):
    name: str
    id: Optional[int] = None
    collections: List[int] = Field([])


class Collection(QShedModel):
    name: str
    database: int
    data: Optional[List] = Field([])
    entity: Optional[int]
    id: Optional[int] = None
    query: Optional[Dict] = Field({})
    limit: Optional[int] = None
    