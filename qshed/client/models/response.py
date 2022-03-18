import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Optional

from .data import Schedule


class Response(BaseModel):
    ok: bool = True
    message: str = ""

    @property
    def content_(self):
        return self._decode(self.content)

    def _decode(self, content):
        return content


class DictResponse(Response):
    content: Dict[str, str]


class ListResponse(Response):
    content: List[str]


class StrResponse(Response):
    content: str


class IntResponse(Response):
    content: int


class JSONResponse(Response):
    content: Any


class ScheduleResponse(Response):
    content: Optional[Schedule] = None


class SchedulesResponse(Response):
    content: List[Schedule] = []


class TagResponse(Response):
    content: str

    def _decode(self, content):
        return pd.read_json(content)
