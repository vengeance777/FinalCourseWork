from fastapi import FastAPI
app = FastAPI()
from sqlalchemy import create_engine, text
from db import DW
# get put post delete
# uvicorn main:app --port 8001

#Lainauksien määrä valitulta kuukaudelta viikottain
#esim. Hae tammikuun 2003 lainauksien lukumäärä viikottain ryhmiteltynä

@app.get('/api/transactions/weekly-by-month/{month}/{year}')
async def get_transactions_weekly_by_month(dw: DW, month: int, year: int):
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
async def get_transactions_daily_by_month(dw: DW, month: int, year: int):
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
async def get_transactions_monthly_by_year(dw: DW, year: int):
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
async def get_all_time_top_ten(dw: DW):
    _query_str = """
SELECT 
    i.item_id, 
    i.name AS Item_Name, 
    COUNT(f.transaction_id) AS Rentals_Count
FROM 
    fact_rental_transactions f
JOIN 
    dim_rental_items i ON f.item_id = i.item_id
GROUP BY 
    i.item_id
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
async def get_all_time_top_ten_month_year(dw: DW, month: int, year: int):
    _query_str = """
SELECT 
    i.item_id, 
    i.name AS Item_Name, 
    COUNT(f.transaction_id) AS Rentals_Count
FROM 
    fact_rental_transactions f
JOIN 
    dim_rental_items i ON f.item_id = i.item_id
JOIN
    dim_date d ON f.date_key = d.date_key
WHERE 
    d.year = :year AND d.month = :month
GROUP BY 
    i.item_id
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
async def top_acquisition_year(dw: DW, year: int):
    _query_str = """
SELECT 
    MONTH(i.acquisition_date) AS Month, 
    COUNT(i.item_id) AS Items_Added
FROM 
    dim_rental_items i
WHERE 
    YEAR(i.acquisition_date) = :year
GROUP BY 
    MONTH(i.acquisition_date)
ORDER BY 
    Items_Added DESC
LIMIT 1;
    """
    _query = text(_query_str)
    rows = dw.execute(_query, {'year': year})
    data = rows.mappings().all()
    return {'data': data}
