import faker_commerce
from faker import Faker
from sqlalchemy import text
from db3 import get_db


def get_categories(_db):
    _query = "SELECT id FROM categories"
    rows = _db.execute(text(_query))
    ids = []
    for row in rows:
        ids.append(row[0])
    return ids

def insert_categories():
    with get_db() as _db:
        _query = "INSERT INTO categories(name) VALUES(:category)"
        for _category in faker_commerce.CATEGORIES:
            try:
                _db.execute(text(_query), {'category': _category})
                _db.commit()
            except Exception as e:
                print(e)
                _db.rollback()