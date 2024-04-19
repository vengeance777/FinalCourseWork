import uuid
from random import choice

import faker_commerce
from faker import Faker
from sqlalchemy import text

from categories import get_categories
from db3 import get_db
from users import get_users


def _get_items(_db):
    _query = "SELECT id FROM rental_items"
    ids = []
    rows = _db.execute(text(_query))
    for row in rows:
        ids.append(row[0])
    return ids


def insert_features():
    with get_db() as _db:
        _query = "INSERT INTO features(feature) VALUES(:feature)"
        for _feature in ['material', 'size', 'price', 'color']:
            try:
                _db.execute(text(_query), {'feature': _feature})
                _db.commit()
            except Exception as e:
                print(e)
                _db.rollback()


def insert_items():
    with get_db() as _db:
        fake = Faker()
        fake.add_provider(faker_commerce.Provider)
        categories = get_categories(_db)
        _query = "INSERT INTO rental_items(name, description, created_at, serial_number, categories_id) VALUES"
        variables = {}
        for i in range(1000):
            _query += f'(:name{i}, :desc{i}, :created_at{i}, :sn{i}, :categories_id{i}),'
            variables[f'name{i}'] = fake.ecommerce_name()
            variables[f'desc{i}'] = fake.text()
            variables[f'created_at{i}'] = fake.date()
            variables[f'sn{i}'] = str(uuid.uuid4())
            variables[f'categories_id{i}'] = choice(categories)

        _query = _query[:-1]
        _db.execute(text(_query), variables)
        _db.commit()


def _get_features(_db):
    _query = "SELECT id, feature FROM features"
    rows = _db.execute(text(_query))
    _features = []
    for row in rows:
        _features.append({'id': row[0], 'feature': row[1]})
    return _features


def mix_features_and_items():
    fake = Faker()
    fake.add_provider(faker_commerce.Provider)
    with get_db() as _db:
        colors = ['black', 'cyan', 'yellow', 'white', 'red', 'pink']
        sizes = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', '20x30', '70x3000']

        items = _get_items(_db)
        features = _get_features(_db)
        _query = "INSERT INTO rental_items_has_features(rental_items_id, features_id, value) VALUES(:item_id, :feature_id, :value)"
        for i in range(1000):
            try:
                item_id = choice(items)
                for f in features:
                    if f['feature'] == 'color':
                        value = choice(colors)
                    elif f['feature'] == 'price':
                        value = fake.ecommerce_price(False)
                    elif f['feature'] == 'size':
                        value = choice(sizes)
                    elif f['feature'] == 'material':
                        value = choice(faker_commerce.PRODUCT_DATA['material'])
                    _db.execute(text(_query), {'item_id': item_id, 'feature_id': f['id'], 'value': value})
                    _db.commit()
            except Exception as e:
                print(e)
                _db.rollback()


def rent_items():
    with get_db() as _db:
        fake = Faker()
        users = get_users(_db)
        items = _get_items(_db)
        _query = "INSERT INTO rental_transactions(created_at, due_date, auth_users_id, rental_items_id) VALUES"
        variables = {}
        for i in range(1000):
            _query += f'(:created_at{i}, :due_date{i}, :auth_users_id{i}, :rental_items_id{i}),'
            variables[f'created_at{i}'] = fake.date()
            variables[f'due_date{i}'] = fake.date()
            variables[f'auth_users_id{i}'] = choice(users)
            variables[f'rental_items_id{i}'] = choice(items)
        _query = _query[:-1]
        _db.execute(text(_query), variables)
        _db.commit()