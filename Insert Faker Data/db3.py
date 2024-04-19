import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_db1():
    engine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/laplanduas_rental')
    db_session = sessionmaker(bind=engine)
    return db_session()


@contextlib.contextmanager
def get_db():
    _db = None
    try:
        print("###################### inside try")
        engine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/laplanduas_rental')
        db_session = sessionmaker(bind=engine)
        _db = db_session()
        print("############ yield db")
        yield _db
    finally:

        if _db is not None:
            print("###########all done, closing db connection")
            _db.close()