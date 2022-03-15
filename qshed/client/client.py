from typing import List, Dict, Optional
import pandas as pd
import requests
import json
from pydantic import parse_obj_as

from .models import Schedule, Request
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

	def add(
		self, 
		url: str, 
		interval: int, 
		method: str = "get", 
		params: Dict[str, str] = {}, 
		data: Dict[str, Optional[str]] = {}, 
		headers: Dict[str, str] = {}
	) -> Schedule:
		request = Request(
			method=method,
			url=url,
			params=params,
			data=data,
			headers=headers
		)
		schedule = Schedule(
			interval=interval,
			request=request
		)
		ok, resp = self.comms.post(f"scheduler/add", data=schedule.json(exclude_none=True))
		if ok:
			return resp
		else:
			raise Exception(resp)

	def list(self) -> List[Schedule]:
		resp = self.comms.get("scheduler/list")
		schedulers = parse_obj_as(List[Schedule], resp)
		return schedulers


class Collection:
	def __init__(self, comms:Comms, database_name:str, collection_name:str) -> None:
		self.comms = comms
		self.database_name = database_name
		self.name = collection_name

	def get(self, limit:int=10, **params:Dict[str,str]):
		return self.comms.get(f"database/{self.database_name}/{self.name}/get/{limit}", params=params)

	@property
	def data(self):
		return self.get()

	def dataframe(self, limit:int=10, **params):
		data = self.get(limit=limit, **params)
		return pd.DataFrame([flatten_dict(item) for item in data])

	def insert(self, data, mode="single"):
		url = f"database/{self.database_name}/{self.name}/insert"
		
		if mode == "single":
			url = url
		elif mode == "multi":
			url += "/many"
		else:
			raise Exception("Invalid mode")

		resp = self.comms.post(url, data=data)
		return resp

	def delete(self, query:str, mode:str="many"):
		"""
		mode:
			"many" (default) - Will delete all documents that match the query
			"one" - Will delete first document that matches the query
		"""
		url = f"database/{self.database_name}/{self.name}/delete"
		if mode == "many":
			url += "/many"
		elif mode == "one":
			url += "/one"
		else:
			raise Exception("Invalid mode")
		resp = self.comms.post(url, data={"query":query})
		return resp

	def delete_all(self):
		resp = self.comms.post(f"database/{self.database_name}/{self.name}/delete/all", data={"confirm": True})
		return resp


class Database:
	def __init__(self, comms:Comms, database_name:str):
		self.comms = comms
		self.name = database_name

	def __getitem__(self, key):
		return Collection(self.comms, self.name, key)

	def list(self):
		return self.comms.get(f"database/{self.name}/list")


class DatabaseExt:
	def __init__(self, comms:Comms):
		self.comms = comms

	def __getitem__(self, key:str):
		return Database(self.comms, key)

	def list(self):
		data = self.comms.get("database/list")
		return data

	def create(self, database_name:str):
		return self.comms.post("database/create", data={"database_name":database_name})





class QShedClient:
	def __init__(self, gateway_address):
		self.comms = Comms(gateway_address)
		self.database = DatabaseExt(self.comms)
		self.scheduler = SchedulerExt(self.comms)
