============================================================
=====    *** Project implementation instruction ***    =====
============================================================

1. change directory to project 01-data-modeling-i:
$ cd 01-data-modeling-i

2. create visual environment named 'ENV' (only 1st time):
$ python -m venv ENV

3. activate the visual environment:
$ source ENV/bin/activate

4. install required libraries from config file (only 1st time): 
$ pip install -r requirements.txt


============================== NOT REQUIRED ==============================

### Prerequisite when install psycopg2 package:
### - For Debian/Ubuntu users: sudo apt install -y libpq-dev
### - For Mac users(intel cpu): brew install postgresql
### - For Mac users(ARM cpu): arch -arm64 brew install postgresql

### install PostgreSQL connector library in Python:
### pip install psycopg2-binary

============================== NOT REQUIRED ==============================


5. start Postgres and Adminer services by start Docker:
$ docker-compose up

6. open browser to connect postgres and login: http://localhost:8080/
 - System: PostgreSQL
 - Server: postgres
 - Username: postgres
 - Password: postgres
 - Database: postgres


7. create tables:
$ python create_tables.py

8. insert data into tables:
$ python etl.py

9. check the data in browser: http://localhost:8080/


============================== SHUTDOWN STEP ==============================

10. stop Postgres and Adminer services by shutdown Docker:
$ docker-compose down

11. deactivate the visual environment:
$ deactivate

