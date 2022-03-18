import json
from time import sleep

from qshed.client import QShedClient


c = QShedClient("http://localhost:5000")


def test_gateway_ping():
    r = c.gateway.ping()
    assert r == '"ok"'


def test_nosql_database_create():
    r = c.nosql.create("__test_database")
    assert r == "__test_database"
    sleep(1)


def test_nosql_database_list():
    r = c.nosql.list()
    assert "__test_database" in r


def test_nosql_collection_insert_one():
    r = c.nosql["__test_database"]["__test_collection"].insert_one({"data": "test"})
    assert isinstance(r, str)


def test_nosql_collection_list():
    r = c.nosql["__test_database"].list()
    assert "__test_collection" in r


def test_nosql_collection_get():
    r = c.nosql["__test_database"]["__test_collection"].get()
    r = json.loads(r)
    assert "data" in r[-1]
    assert r[-1]["data"] == "test"

