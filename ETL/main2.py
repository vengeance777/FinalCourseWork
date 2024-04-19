import multiprocessing
from sqlalchemy import text
from db2 import get_db  # Ensure this manages your DB sessions.
import calendar
from datetime import datetime



def extract_users(_db):
    # Extracting users from the OLTP system
    query = text("SELECT id AS user_id, username, password, roles_id AS role_id FROM auth_users")
    return _db.execute(query).mappings().all()

def load_users(_dw, users):
    for user in users:
        # Loading users into the OLAP system with the updated table and column names
        _dw.execute(text("""
            INSERT INTO dim_auth_users (user_id, username, password, role_id) 
            VALUES (:user_id, :username, :password, :role_id)
            """), {
                'user_id': user['user_id'],
                'username': user['username'],
                'password': user['password'],
                'role_id': user['role_id']  # Corrected to match the extracted key
            })
        _dw.commit()

def extract_category(_db):
    # Extracting users from the OLTP system
    query = text("SELECT id AS category_id, name FROM categories")
    return _db.execute(query).mappings().all()

def load_category(_dw, users):
    for user in users:
        # Loading users into the OLAP system with the updated table and column names
        _dw.execute(text("""
            INSERT INTO category_dim (category_id, name) 
            VALUES (:category_id, :name)
            """), {
                'category_id': user['category_id'],
                'name': user['name']
            })
        _dw.commit()

def extract_items(_db):
    # Extracting items from the OLTP system
    query = text("""
        SELECT 
            id AS item_id, 
            name, 
            description, 
            serial_number, 
            categories_id AS category_id, 
            created_at AS acquisition_date 
        FROM rental_items
    """)
    return _db.execute(query).mappings().all()


def load_items(_dw, items):
    for item in items:
        _dw.execute(text("""
            INSERT INTO dim_rental_items (item_id, name, description, serial_number, category_id, acquisition_date) 
            VALUES (:item_id, :name, :description, :serial_number, :category_id, :acquisition_date)
            """), {
                'item_id': item['item_id'],
                'name': item['name'],
                'description': item.get('description', None),  # Using .get for optional fields
                'serial_number': item['serial_number'],
                'category_id': item['category_id'],
                'acquisition_date': item['acquisition_date']  # Corrected here
            })
        _dw.commit()

def extract_date(_db):
    # Extracting dates from the OLTP system, including year, quarter, month, week, day_of_month, day_of_week, and day_name
    query = text("""
        SELECT 
            created_at AS date_key, 
            QUARTER(created_at) AS quarter,
            YEAR(created_at) AS year
            FROM rental_transactions
    """)
    return _db.execute(query).mappings().all()

def load_date(_dw, items):
    for item in items:
        # Check if the record already exists
        result = _dw.execute(text("""
            SELECT COUNT(*) FROM dim_date WHERE date_key = :date_key
            """), {
            'date_key': item['date_key']
        }).scalar()

        # If the record doesn't exist, insert it
        if result == 0:
            _dw.execute(text("""
                INSERT INTO dim_date (
                    date_key, quarter, month, month_name, week, 
                    day_of_month, day_of_week, year, day_name
                ) VALUES (
                    :date_key, 
                    QUARTER(:date_key), 
                    MONTH(:date_key),
                    MONTHNAME(:date_key), 
                    WEEK(:date_key, 1),
                    DAY(:date_key), 
                    DAYOFWEEK(:date_key), 
                    YEAR(:date_key),
                    DAYNAME(:date_key)
                )
                """), {
                    'date_key': item['date_key']
                })
            _dw.commit()


def extract_fact(_db):
    # Extracting transactions from the OLTP system
    query = text("""
        SELECT 
            id AS transaction_id,  
            due_date, 
            returned_at, 
            auth_users_id AS user_id, 
            rental_items_id AS item_id,
            DATE(created_at) AS date_key  # Extracting date_key from created_at
        FROM 
            rental_transactions
    """)
    return _db.execute(query).mappings().all()


def load_fact(_dw, transactions):
    for transaction in transactions:
        _dw.execute(text("""
            INSERT INTO fact_rental_transactions (
                transaction_id,  
                due_date, 
                returned_at, 
                user_id, 
                item_id,
                date_key  # Including date_key in the INSERT statement
            ) VALUES (
                :transaction_id, 
                :due_date, 
                :returned_at, 
                :user_id, 
                :item_id,
                :date_key  # Binding date_key value
            )
            """), {
                'transaction_id': transaction['transaction_id'],
                'due_date': transaction['due_date'],
                'returned_at': transaction['returned_at'],
                'user_id': transaction['user_id'],
                'item_id': transaction['item_id'],
                'date_key': transaction['date_key']  # Ensuring date_key is passed correctly
            })
        _dw.commit()

def user_etl_process(child_conn):
    with get_db() as _db:
        users = extract_users(_db)
    with get_db(_type='dw') as _dw:
        load_users(_dw, users)
    child_conn.send("Users ETL completed")
    child_conn.close()


def category_etl_process(child_conn):
    with get_db() as _db:
        users = extract_category(_db)
    with get_db(_type='dw') as _dw:
        load_category(_dw, users)
    child_conn.send("Category ETL completed")
    child_conn.close()

def items_etl_process(child_conn):
    with get_db() as _db:
        users = extract_items(_db)
    with get_db(_type='dw') as _dw:
        load_items(_dw, users)
    child_conn.send("Items ETL completed")
    child_conn.close()


def date_etl_process(child_conn):
    with get_db() as _db:
        users = extract_date(_db)
    with get_db(_type='dw') as _dw:
        load_date(_dw, users)
    child_conn.send("Dates ETL completed")
    child_conn.close()


def fact_etl_process(child_conn):
    with get_db() as _db:
        users = extract_fact(_db)
    with get_db(_type='dw') as _dw:
        load_fact(_dw, users)
    child_conn.send("Fact ETL completed")
    child_conn.close()

def start_and_wait_for_processes(process_list):
    parent_connections = []
    processes = []

    for function in process_list:
        parent_conn, child_conn = multiprocessing.Pipe()
        process = multiprocessing.Process(target=function, args=(child_conn,))
        processes.append(process)
        parent_connections.append(parent_conn)
        process.start()

    # Wait for all processes in the current batch to complete
    for process in processes:
        process.join()

    # Collecting results
    for parent_conn in parent_connections:
        print(parent_conn.recv())


def main():
    # Splitting processes into dimension processes and fact processes
    dimension_processes = [user_etl_process, category_etl_process, items_etl_process, date_etl_process]
    fact_processes = [fact_etl_process]

    # Start and wait for dimension processes to finish
    start_and_wait_for_processes(dimension_processes)

    # After dimension processes have finished, start and wait for fact processes
    start_and_wait_for_processes(fact_processes)


if __name__ == '__main__':
    main()
