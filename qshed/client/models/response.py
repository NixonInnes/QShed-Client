import json
import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Optional, Any

from .data import Schedule, SQLEntity


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


class TagResponse(Response):
    content: str

    def _decode(self, content):
        return pd.read_json(content)


class SQLEntityResponse(JSONResponse):

    def _decode(self, content):
        return SQLEntity.parse_obj(super()._decode(content))