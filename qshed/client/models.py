from typing import Dict, Optional
from pydantic import BaseModel, Field, validator

from .utils import string_hash


class Request(BaseModel):
    url: str
    method: str
    params: Dict[str, str] = Field({})
    data: Dict[str, str] = Field({})
    headers: Dict[str, str] = Field({})

    @validator("method")
    def validate_method(cls, method: str):
        if method.lower() not in ("get", "post"):
            raise ValueError("must be either 'get' or 'post'")
        return method.lower()

    def get_collection_id(self):
        return string_hash(self.json())


class Schedule(BaseModel):
    request: Request
    interval: int
    id: Optional[str]
    name: Optional[str]
    next_run: Optional[float]

    def get_collection_id(self):
        return self.request.get_collection_id()
