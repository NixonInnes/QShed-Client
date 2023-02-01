import json
import pandas as pd
from typing import Generic, TypeVar, Optional, List

from pydantic import BaseModel, validator, ValidationError
from pydantic.generics import GenericModel

from . import data as dataModels
from ..utils import zip_str, unzip_str


DataType = TypeVar("DataType")


class Error(BaseModel):
    code: int
    message: str
    _is_error: bool = True


class Response(GenericModel, Generic[DataType]):
    data: Optional[DataType]
    error: Optional[Error]

    @validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


def error(code, message):
    return Response(error=Error(code=code, message=message))


EntityResponse = Response[dataModels.Entity]
EntityListResponse = Response[List[dataModels.Entity]]


def ts_response_json_loads(v):
    dic = json.loads(v)
    dic["data"]["data"] = pd.read_json(unzip_str(dic["data"]["data"]))
    return dic


def ts_list_response_json_loads(v):
    dic = json.loads(v)
    for ts in dic["data"]:
        ts["data"] = pd.read_json(unzip_str(ts["data"]))
    return dic


class TimeseriesResponse(Response[dataModels.Timeseries]):
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {pd.DataFrame: lambda df: zip_str(df.to_json())}
        json_loads = ts_response_json_loads


class TimeseriesListResponse(Response[List[dataModels.Timeseries]]):
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {pd.DataFrame: lambda df: zip_str(df.to_json())}
        json_loads = ts_list_response_json_loads


CollectionResponse = Response[dataModels.Collection]
CollectionListResponse = Response[List[dataModels.Collection]]
CollectionDatabaseResponse = Response[dataModels.CollectionDatabase]
CollectionDatabaseListResponse = Response[List[dataModels.CollectionDatabase]]
