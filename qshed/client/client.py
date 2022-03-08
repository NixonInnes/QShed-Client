import pandas as pd
import requests
import json
from collections.abc import MutableMapping


def _flatten_dict_gen(d, parent_key, sep):
    for k, v in d.items():
        k = k.replace(sep, "").replace(" ", "_")
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '.'):
    return dict(_flatten_dict_gen(d, parent_key, sep))


class Scheduler:
	def __init__(self, comms):
		self.comms = comms

	def add(self, url, method="get", params={}, data={}, headers={}):
		data = dict(
			method=method,
			url=url,
			params=params,
			data=data,
			headers=headers
		)
		resp = self.comms.post(f"scheduler/add", data=json.dumps(data))
		return resp

	def list(self):
		resp = self.comms.get("scheduler/list")
		return resp

class Collection:
	def __init__(self, comms, database_name, collection_name):
		self.comms = comms
		self.database_name = database_name
		self.name = collection_name

	def get(self, limit=10, **params):
		return self.comms.get(f"database/{self.database_name}/{self.name}/get/{limit}", params=params)

	@property
	def data(self):
		return self.get()

	def dataframe(self, limit=10, **params):
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

	def delete(self, query, mode="many"):
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
	def __init__(self, comms, database_name):
		self.comms = comms
		self.name = database_name

	def __getitem__(self, key):
		return Collection(self.comms, self.name, key)

	def list(self):
		return self.comms.get(f"database/{self.name}/list")


class DatabaseExt:
	def __init__(self, comms):
		self.comms = comms

	def __getitem__(self, key):
		return Database(self.comms, key)

	def list(self):
		data = self.comms.get("database/list")
		return data

	def create(self, database_name):
		return self.comms.post("database/create", data={"database_name":database_name})


class Comms:
	def __init__(self, address):
		if not address.endswith("/"):
			address += "/"
		self.address = address
		self.headers = {'Content-Type': 'application/json'}

	def get(self, url_ext, params={}):
		resp = requests.get(self.address+url_ext, params=params)
		try:
			return resp.json()
		except:
			raise Exception(f"Error {resp.status_code}: {resp.content}")

	def post(self, url_ext, params={}, data={}):
		resp = requests.post(self.address+url_ext, data=json.dumps(data), params=params, headers=self.headers)
		try:
			return resp.json()
		except:
			raise Exception(f"Error {resp.status_code}: {resp.content}")


class QShedClient:
	def __init__(self, gateway_address):
		self.comms = Comms(gateway_address)
		self.database = DatabaseExt(self.comms)
		self.scheduler = Scheduler(self.comms)
