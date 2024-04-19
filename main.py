from passlib.hash import pbkdf2_sha512 as pl
from db import DW
from pydantic import BaseModel
from sqlalchemy.sql import text
import jwt
from fastapi import Header, HTTPException
from fastapi import Depends, FastAPI
from typing import Annotated
app = FastAPI()
# get put post delete
# uvicorn main:app --port 8001
class RegisterRequest(BaseModel):
    username: str
    password: str

SECRET_KEY = 'sdfLksjfsdflkjSdflksjflksdjflksjflkdjflk32j0329834239243up423834230294i24k234_lk3m2k1342+23426423_423fkl342+243'

def require_login(dw: DW, authorization=Header(default=None, alias='api_key')):
    try:
        if authorization is not None and len(authorization.split(' ')) == 2:
            validated = jwt.decode(authorization.split(' ')[1], SECRET_KEY, algorithms=['HS512'])
            user = dw.execute(text('SELECT username FROM auth_users_rest WHERE user_id = :user_id'), {'user_id': validated['user_id']}).mappings().first()

            if user is None:
                raise HTTPException(detail='unauthorized', status_code=401)
            return user
        else:
            raise HTTPException(detail='unauthorized', status_code=401)

    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)


# type alias
LoggedInUser = Annotated[dict, Depends(require_login)]

@app.get('/api/account')
async def get_account(dw: DW, logged_in_user = Depends(require_login)):
    return logged_in_user


""" @app.get('/api/account')
async def get_account(dw: DW, authorization: str = Header(None, alias='api_key')):
    try:
        split_header = authorization.split(' ')
        if len(split_header) == 2 and split_header[0] == 'Bearer':
            token = split_header[1]
            validated = jwt.decode(token, SECRET_KEY, algorithms=['HS512'])
            user = dw.execute(text('SELECT username FROM auth_users_rest WHERE user_id = :user_id'),
                              {'user_id': validated['user_id']}).mappings().first()
            if user is None:
                raise HTTPException(detail='user not found', status_code=404)
            return user
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500) """

@app.post('/api/login')
async def login(dw: DW, req: RegisterRequest):
    # 1. hakee kayttajaa usernamen perusteella (username on yksilollinen, joten niita loytyy vain yksi tai 0 kappaletta)

    _query_str = "SELECT * FROM auth_users_rest WHERE username = :username"
    _query = text(_query_str)
    user = dw.execute(_query, {'username': req.username}).mappings().first()
    if user is None:
        raise HTTPException(detail='user not found', status_code=404)

    password_correct = pl.verify(req.password, user['password'])
    if password_correct:
        token = jwt.encode({'user_id': user['user_id']}, SECRET_KEY, algorithm='HS512')
        return {'token': token}
    raise HTTPException(detail='user not found', status_code=404)

@app.post('/api/register')
async def register(dw: DW, req: RegisterRequest):
    try:
        _query_str = "INSERT INTO auth_users_rest (username, password) VALUES(:username, :password)"
        _query = text(_query_str)
        user = dw.execute(_query, {'username': req.username, 'password': pl.hash(req.password)})
        dw.commit()
        return {'username': req.username, 'user_id': user.lastrowid}
    except Exception as e:
        dw.rollback()
        print(e)  # This prints the exception to the console or logs
        raise HTTPException('Error registering user')


#Lainauksien määrä valitulta kuukaudelta viikottain
#esim. Hae tammikuun 2003 lainauksien lukumäärä viikottain ryhmiteltynä

@app.get('/api/transactions/weekly-by-month/{month}/{year}')
async def get_transactions_weekly_by_month(dw: DW, month: int, year: int, logged_in_user: LoggedInUser):


    _query_str = ("""
        SELECT dim_date.month AS Month, COUNT(fact_rental_transactions.transaction_id) AS transaction_count 
        FROM fact_rental_transactions 
        INNER JOIN dim_date ON dim_date.date_key = fact_rental_transactions.date_key
        WHERE dim_date.year = :year AND dim_date.month = :month 
        GROUP BY dim_date.week, dim_date.month
        ORDER BY dim_date.week DESC
    """)
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year, 'month': month})
    data = rows.mappings().all()
    return {'data': data}


#Lainauksien märää valitulta kuukaudelta päivittäin
#esim. Hae tammikuun 2004 lainauksen lukumäärä päivittäin ryhmiteltynä
@app.get('/api/transactions/daily-by-month/{month}/{year}')
async def get_transactions_daily_by_month(dw: DW, month: int, year: int, logged_in_user: LoggedInUser):
    _query_str = ("""
        SELECT dim_date.day_of_month AS Day_Of_Month, COUNT(fact_rental_transactions.transaction_id) AS transaction_count 
        FROM fact_rental_transactions 
        INNER JOIN dim_date ON dim_date.date_key = fact_rental_transactions.date_key
        WHERE dim_date.year = :year AND dim_date.month = :month 
        GROUP BY dim_date.day_of_month
        ORDER BY dim_date.day_of_month DESC
    """)
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year, 'month': month})
    data = rows.mappings().all()
    return {'data': data}

#Lainauksien määrä valitulta vuodelta kuukausittain
#esim. Hae vuoden 2003 lainauksien määrä kuukausittain ryhmiteltynä
#Fix
@app.get('/api/transactions/monthly-by-year/{year}')
async def get_transactions_monthly_by_year(dw: DW, year: int, logged_in_user: LoggedInUser):
    _query_str = ("""
        SELECT dim_date.month AS Month, COUNT(fact_rental_transactions.transaction_id) AS transaction_count 
        FROM fact_rental_transactions 
        INNER JOIN dim_date ON dim_date.date_key = fact_rental_transactions.date_key
        WHERE dim_date.year = :year
        GROUP BY dim_date.month
        ORDER BY dim_date.month DESC
    """)
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year})
    data = rows.mappings().all()
    return {'data': data}

#Kaikkien aikojen top 10. lainatuimmat tavarat
@app.get('/api/transactions/all_time_top_ten')
async def get_all_time_top_ten(dw: DW, logged_in_user: LoggedInUser):
    _query_str = """
SELECT 
    dim_rental_items.item_id, 
    dim_rental_items.name AS Item_Name, 
    COUNT(fact_rental_transactions.transaction_id) AS Rentals_Count
FROM 
    fact_rental_transactions
JOIN 
    dim_rental_items ON fact_rental_transactions.item_id = dim_rental_items.item_id
GROUP BY 
    dim_rental_items.item_id
ORDER BY 
    Rentals_Count DESC
LIMIT 10;
    """
    _query = text(_query_str)
    rows = dw.execute(_query)
    data = rows.mappings().all()
    return {'data': data}

#Kaikkien aikojen top 10. lainatuimmat tavarat (kuukausittain)

@app.get('/api/transactions/all-time-top-ten-month-year/{month}/{year}')
async def get_all_time_top_ten_month_year(dw: DW, month: int, year: int, logged_in_user: LoggedInUser):
    _query_str = """
SELECT 
    dim_rental_items.item_id, 
    dim_rental_items.name AS Item_Name, 
    COUNT(fact_rental_transactions.transaction_id) AS Rentals_Count
FROM 
    fact_rental_transactions
JOIN 
    dim_rental_items ON fact_rental_transactions.item_id = dim_rental_items.item_id
JOIN
    dim_date ON fact_rental_transactions.date_key = dim_date.date_key
WHERE 
    dim_date.year = :year AND dim_date.month = :month
GROUP BY 
    dim_rental_items.item_id
ORDER BY 
    Rentals_Count DESC
LIMIT 10;

    """
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year, 'month': month})
    data = rows.mappings().all()
    return {'data': data}

#Selvitä missä kuussa tavaroita lisätään järjestelmään eniten valittuna vuonna
@app.get('/api/transactions/top-acquisition-month-per-year/{year}')
async def top_acquisition_year(dw: DW, year: int, logged_in_user: LoggedInUser):
    _query_str = """
SELECT 
    MONTH(dim_rental_items.acquisition_date) AS Month, 
    COUNT(dim_rental_items.item_id) AS Items_Added
FROM 
    dim_rental_items
WHERE 
    YEAR(dim_rental_items.acquisition_date) = :year
GROUP BY 
    MONTH(dim_rental_items.acquisition_date)
ORDER BY 
    Items_Added DESC
LIMIT 1;

    """
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year})
    data = rows.mappings().all()
    return {'data': data}