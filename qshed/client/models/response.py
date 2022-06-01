import json
import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Optional, Any

.data import as dataModels
from ..utils import unzip_str


class Response(BaseModel):
    __DataModel = None
    __multi = False
    ok: bool = True
    message: str = ""
    content: Optional[Any] = None

    def parse_content(self):
        return self.content

    def decode(self):
        if self.content is None:
            return None
        if self.__multi:
            return [
            self.__DataModel.parse_obj(obj)
            for obj in self.parse_content()
        ]
        return self.__DataModel.parse_obj(
            self.parse_content()
        )


class ScheduleResponse(Response):
    __DataModel = dataModels.Schedule
    content: Optional[dataModels.Schedule] = None


class ListScheduleResponse(Response):
    __DataModel = dataModels.Schedule
    __multi = True
    content: List[dataModels.Schedule] = []


class TimeseriesResponse(Response):
    __DataModel = dataModels.Timeseries
    content: str

    def parse_content(self):
        return pd.read_json(unzip_str(self.content))


class EntityResponse(Response):
    __DataModel = dataModels.Entity
    content: Optional[dataModels.Entity] = None


class EntityListResponse(Response):
    __DataModel = dataModels.Entity
    __multi = True
    content: List[dataModels.Entity] = []


class CollectionDatabaseResponse(Response):
    __DataModel = dataModels.CollectionDatabase
    content = Optional[dataModels.CollectionDatabase] = None


class CollectionDatabaseListResponse(Response):
    __DataModel = dataModels.CollectionDatabase
    __multi = True
    content: List[dataModels.CollectionDatabase] = []


class CollectionResponse(Response):
    __DataModel = dataModels.Collection
    content: Optional[dataModels.Collection] = None


class CollectionListResponse(Response):
    __DataModel = dataModels.Collection
    __multi = True
    content: List[dataModels.Collection] = []



