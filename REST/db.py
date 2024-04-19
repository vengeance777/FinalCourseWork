from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi import Depends
from typing import Annotated

engine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/rentals_schema_OLAP_3')
dw_session = sessionmaker(bind=engine)

def get_dw():
    _dw = None
    try:
        _dw = dw_session()
        yield _dw
    finally:
        if _dw is not None:
            _dw.close()

# Dependency
DW = Annotated[Session, Depends(get_dw)]