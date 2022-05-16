import json
import yaml
import pandas as pd
from typing import Dict, Optional
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


class DataFrameModel(QShedModel):
    json_str: str

    def parse(self):
        return pd.read_json(self.json_str)


class SQLEntity(QShedModel):
    id: int
    name: str
    data_: str
    type: int

    __types = {
        0: str,
        1: int,
        2: float,
        3: json.loads
    }

    def parse(self):
        return self.__types[self.type](self.data_)

    @property
    def data(self):
        return self.parse()
