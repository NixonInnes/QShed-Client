from typing import List, Dict, Optional
import pandas as pd
import requests
import json
from pydantic import parse_obj_as

from .decorators import typed_response
from .models import data as dataModels, response as responseModels
from .utils import flatten_dict


class Comms:
	def __init__(self, address: str):
		if not address.endswith("/"):
			address += "/"
		self.address = address
		self.headers = {'Content-Type': 'application/json'}

	def get(self, url_ext:str, params:dict={}):
		resp = requests.get(self.address+url_ext, params=params)
		try:
			return resp.json()
		except:
			raise Exception(f"Error {resp.status_code}: {resp.content}")

	def post(self, url_ext:str, params:dict={}, data:dict={}):
		if not isinstance(data, str):
			data = json.dumps(data)
		resp = requests.post(
			self.address+url_ext, 
			data=data, 
			params=params, 
			headers=self.headers)
		try:
			return resp.json()
		except:
			raise Exception(f"Error {resp.status_code}: {resp.content}")


class SchedulerExt:
	def __init__(self, comms:Comms) -> None:
		self.comms = comms

	@typed_response(response_model=responseModels.StrResponse)
	def add(
		self, 
		url: str, 
		interval: int, 
		method: str = "get", 
		params: Dict[str, str] = {}, 
		data: Dict[str, Optional[str]] = {}, 
		headers: Dict[str, str] = {}
	):
		request = dataModels.Request(
			method=method,
			url=url,
			params=params,
			data=data,
			headers=headers
		)
		schedule = dataModels.Schedule(
			interval=interval,
			request=request
		)
		return self.comms.post(f"scheduler/add", data=schedule.json(exclude_none=True))

	@typed_response(response_model=responseModels.SchedulesResponse)
	def list(self):
		return self.comms.get("scheduler/list")


class Database:
	def __init__(self, comms:Comms, database_name:str):
		self.comms = comms
		self.name = database_name

	def __getitem__(self, key:str):
		return Collection(self.comms, self.name, key)

	@typed_response(response_model=responseModels.ListResponse)
	def list(self):
		return self.comms.get(f"database/{self.name}/list")


class DatabaseExt:
	def __init__(self, comms:Comms):
		self.comms = comms

	def __getitem__(self, key:str):
		return Database(self.comms, key)

	@typed_response(response_model=responseModels.ListResponse)
	def list(self):
		return self.comms.get("database/list")

	@typed_response(response_model=responseModels.StrResponse)
	def create(self, database_name:str):
		return self.comms.post("database/create", params={"database_name":database_name})


class Collection:
	def __init__(self, comms:Comms, database_name:str, collection_name:str) -> None:
		self.comms = comms
		self.database_name = database_name
		self.name = collection_name

	@typed_response(response_model=responseModels.JSONResponse)
	def get(self, limit:int=10, query:Optional[Dict[str,str]]={}):
		return self.comms.get(f"database/{self.database_name}/{self.name}/get/{limit}")

	@property
	def data(self):
		return self.get()

	def dataframe(self, limit:int=10, **params):
		data = self.get(limit=limit, **params)
		return pd.DataFrame([flatten_dict(item) for item in data])

	@typed_response(response_model=responseModels.DictResponse)
	def insert_one(self, data:dict):
		return self.comms.post(f"database/{self.database_name}/{self.name}/insert", data=data)

	@typed_response(response_model=responseModels.ListResponse)
	def insert_many(self, data:list):
		return self.comms.post(f"database/{self.database_name}/{self.name}/insert/many", data=data)

	@typed_response(response_model=responseModels.Response)
	def delete_one(self, query:str):
		return self.comms.post(f"database/{self.database_name}/{self.name}/delete/one", data={"query":query})

	@typed_response(response_model=responseModels.IntResponse)
	def delete_many(self, query:str):
		return self.comms.post(f"database/{self.database_name}/{self.name}/delete/many", data={"query":query})

	@typed_response(response_model=responseModels.IntResponse)
	def delete_all(self, confirm:bool=False):
		return self.comms.post(f"database/{self.database_name}/{self.name}/delete/all", params={"confirm": confirm})


class QShedClient:
	def __init__(self, gateway_address:str):
		self.comms = Comms(gateway_address)
		self.database = DatabaseExt(self.comms)
		self.scheduler = SchedulerExt(self.comms)
