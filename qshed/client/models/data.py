import json
import yaml
from pandas import DataFrame
from typing import Dict, Optional, List, DateTime
from pydantic import BaseModel, Field, validator

from ..utils import string_hash


class QShedModel(BaseModel):
    def yaml(self) -> str:
        return yaml.dump(self.dict())


class Request(QShedModel):
    url: str
    method: str
    params: Dict[str, str] = Field({})
    data: Dict[str, str] = Field({})
    headers: Dict[str, str] = Field({})

    @validator("method")
    def validate_method(cls, method: str) -> str:
        if method.lower() not in ("get", "post"):
            raise ValueError("must be either 'get' or 'post'")
        return method.lower()

    def get_collection_id(self) -> str:
        return string_hash(self.json())


class Schedule(QShedModel):
    request: Request
    interval: int
    id: Optional[str]
    name: Optional[str]
    next_run: Optional[float]

    def get_collection_id(self) -> str:
        return self.request.get_collection_id()


class Entity(QShedModel):
    id: Optional[int] = None
    name: str
    display_name: Optional[str]
    data_: str
    type: int
    parent: Optional[int]
    children: List[int]

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


class Timeseries(QShedModel):
    name: str
    data: DateFrame
    id: Optional[int] = None
    start: Optional[DateTime] = None
    end: Optional[DateTime] = None


class CollectionDatabase(QShedModel):
    name: str
    id: Optional[int] = None
