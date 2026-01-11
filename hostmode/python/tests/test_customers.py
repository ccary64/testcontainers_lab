import os

import pytest
from testcontainers.postgres import PostgresContainer

from customers import customers

postgres = PostgresContainer("postgres:16-alpine")
postgres.ports = {}
postgres.with_kwargs(network_mode="host")


@pytest.fixture(scope="module", autouse=True)
def setup():
    postgres.start()
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5432"
    os.environ["DB_USERNAME"] = postgres.username
    os.environ["DB_PASSWORD"] = postgres.password
    os.environ["DB_NAME"] = postgres.dbname
    customers.create_table()
    yield
    postgres.stop()


@pytest.fixture(autouse=True)
def setup_data():
    customers.delete_all_customers()


EXPECTED_CUSTOMERS = 2


def test_get_all_customers():
    customers.create_customer("Siva", "siva@gmail.com")
    customers.create_customer("James", "james@gmail.com")
    customers_list = customers.get_all_customers()
    assert len(customers_list) == EXPECTED_CUSTOMERS


def test_get_customer_by_email():
    customers.create_customer("John", "john@gmail.com")
    customer = customers.get_customer_by_email("john@gmail.com")
    assert customer is not None
    assert customer.name == "John"
    assert customer.email == "john@gmail.com"
