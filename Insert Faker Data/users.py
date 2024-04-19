import uuid
from random import choice

from faker import Faker
from passlib.context import CryptContext
from sqlalchemy import text

from db3 import get_db


def insert_roles():
    with get_db() as _db:

        _query = "INSERT INTO roles(role) VALUES(:role)"
        # insert into roles(role) values('normaluser');
        # insert into roles(role) values('admin');

        for _role in ['normaluser', 'admin', 'moderator']:
            try:
                _db.execute(text(_query), {'role': _role})
                _db.commit()
            except Exception as e:
                print(e)
                _db.rollback()


def get_users(_db):
    _query = "SELECT id FROM auth_users"
    rows = _db.execute(text(_query))
    ids = []
    for row in rows:
        ids.append(row[0])
    return ids


def _get_roles(_db):
    _query = "SELECT id, role FROM roles"
    rows = _db.execute(text(_query))
    role_ids = []
    for row in rows:
        role_ids.append(row[0])

    return role_ids


def insert_users(num_of_rows=10):
    bcrypt_context = CryptContext(schemes=['bcrypt'])
    fake = Faker()
    with get_db() as _db:
        role_ids = _get_roles(_db)
        variables = {}
        _query = "INSERT INTO auth_users(username, password, roles_id) VALUES"
        for i in range(num_of_rows):
            pwd = bcrypt_context.hash('salasana')
            _random_str = str(uuid.uuid4())
            # INSERT INTO auth_users(username, password, roles_id) VALUES('juhani', 'salasana', 11), ('keijo', 'salasana', 13),
            _query += f'(:username{i}, :password{i}, :roles_id{i}),'
            variables[f'username{i}'] = f'{fake.first_name()}-{_random_str}'
            variables[f'password{i}'] = pwd
            variables[f'roles_id{i}'] = choice(role_ids)
        _query = _query[:-1]
        _db.execute(text(_query), variables)
        _db.commit()