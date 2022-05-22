import json
import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Optional, Any

from .data import Schedule, SQLEntity, TimeseriesRecord
from ..utils import unzip_str


class Response(BaseModel):
    ok: bool = True
    message: str = ""
    content: Optional[Any] = None

    @property
    def content_(self):
        return self._decode(self.content)

    def _decode(self, content):
        return content


class BoolResponse(Response):
    content: bool


class DictResponse(Response):
    content: Dict[str, str]


class StrListResponse(Response):
    content: List[str]


class IntListResponse(Response):
    content: List[int]


class StrResponse(Response):
    content: str


class IntResponse(Response):
    content: int


class JSONResponse(Response):
    content: str

    def _decode(self, content):
        return json.loads(content)


class ScheduleResponse(Response):
    content: Optional[Schedule] = None


class SchedulesResponse(Response):
    content: List[Schedule] = []


class TimeseriesResponse(Response):
    content: str

    def _decode(self, content):
        return pd.read_json(unzip_str(content))


class SQLEntityResponse(JSONResponse):

    def _decode(self, content):
        return SQLEntity.parse_obj(super()._decode(content))


class SQLEntityListResponse(JSONResponse):

    def _decode(self, content) -> List:
        return [SQLEntity.parse_obj(entity) for entity in super()._decode(content)]


class TimeseriesRecordResponse(JSONResponse):

    def _decode(self, content):
        return TimeseriesRecord.parse_obj(super()._decode(content))


class TimeseriesRecordListResponse(JSONResponse):

    def _decode(self, content) -> List:
            return [TimeseriesRecord.parse_obj(record) for record in super()._decode(content)]
