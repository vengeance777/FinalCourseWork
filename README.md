Steps
1. Import OLAP SQL create script and OLTP files to phpmyadmin from Import --> File to import (Databases folder)
2. Add data to OLTP with files in "Insert Faker Data" by running main3 (Insert Faker Data Folder)
3. Perform ETL with files from ETL by running main2 (ETL Folder)
4. Run main.py and open rest API interface to search for pre-defied queries (REST Folder)


If required upon running, remember to separately install requirements for each folder
by navigating with cd command to folder and install with pip

pip install -r requirements1.txt
pip install -r requirements2.txt
pip install -r requirements3.txt

#### laplanduas_rental.sql might need to be imported to database server instead  of empty OLTP.txt create script file if running faker gives errors during import users (this data was added during lectures and contains full db dumb) 
#### There is possibility separate virtual environment needs to be created for each folder and their requirements to avoid conflicts to get rid of above problem
