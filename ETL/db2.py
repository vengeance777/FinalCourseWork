import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

@contextlib.contextmanager
def get_db(_type='db'):
    _db = None
    try:
        cnx_str = 'mysql+mysqlconnector://root:@127.0.0.1/laplanduas_rental'
        if _type == 'dw':
            cnx_str = 'mysql+mysqlconnector://root:@127.0.0.1/rentals_schema_OLAP_3'
        engine = create_engine(cnx_str)
        db_session = sessionmaker(bind=engine)

        _db = db_session()
        yield _db
    except Exception as e:
        print(e)
    finally:
        if _db is not None:
            _db.close()