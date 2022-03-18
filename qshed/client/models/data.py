import yaml
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
