from __future__ import annotations

import json
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List, Any, Callable
from pydantic import BaseModel, Field, validator, create_model  # , computed_field

from ..utils import string_hash, zip_str, unzip_str

type_map = {
    str: "string",
    float: "number",
    int: "integer",
}


class QShedModel(BaseModel):
    _is_error = False

    def yaml(self) -> str:
        return yaml.dump(self.dict())


class DataModel(BaseModel):
    entity_id: int | None

    @classmethod
    def create_definition(cls, name, **attributes: dict[str, Callable]) -> DataModel:
        return create_model(
            name,
            **{attr: (type_, ...) for attr, type_ in attributes.items()},
            __base__=cls,
        )

    @classmethod
    def get_definition(cls):
        return DataModelDefinition(
            name=cls.__name__,
            attributes=[
                DataModelAttribute(
                    name=attr,
                    type=type_map[type_]
                ) for attr,type_ in cls.__annotations__.items() 
            ]
        )

    


class Entity(QShedModel):
    pass


class DataModelAttribute(BaseModel):
    name: str
    type: str


class DataModelDefinition(BaseModel):
    name: str
    attributes: list[DataModelAttribute]


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
        json_encoders = {pd.DataFrame: lambda df: zip_str(df.to_json())}
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
