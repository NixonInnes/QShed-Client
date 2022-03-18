from pydantic import BaseModel
from typing import Union, Dict


class MongoQuery(BaseModel):
    key: str
    query: Union[str, Dict[str, str]]
