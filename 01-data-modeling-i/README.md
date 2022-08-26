# Data modeling i

## Project implementation instruction
<br>

### Implementation steps

##### 1. change directory to project 01-data-modeling-i:
```sh
$ cd 01-data-modeling-i
```

##### 2. create visual environment named 'ENV' (only 1st time):
```sh
$ python -m venv ENV
```

##### 3. activate the visual environment:
```sh
$ source ENV/bin/activate
```

##### 4. install required libraries from config file (only 1st time): 
```sh
$ pip install -r requirements.txt
```

<br>

~~*NOT REQUIRED*~~

~~*Prerequisite when install psycopg2 package:*~~<br>
~~* - For Debian/Ubuntu users: sudo apt install -y libpq-dev*~~<br>
~~* - For Mac users(intel cpu): brew install postgresql*~~<br>
~~* - For Mac users(ARM cpu): arch -arm64 brew install postgresql*~~<br>
<br>
~~*install PostgreSQL connector library in Python:*~~<br>
~~*$ pip install psycopg2-binary*~~<br>
<br>


##### 5. start Postgres and Adminer services by start Docker:
```sh
$ docker-compose up
```

##### 6. open browser to connect postgres and login: http://localhost:8080/
 - System: PostgreSQL
 - Server: postgres
 - Username: postgres
 - Password: postgres
 - Database: postgres


##### 7. create tables:
```sh
$ python create_tables.py
```

##### 8. insert data into tables:
```sh
$ python etl.py
```

##### 9. check the data in browser: http://localhost:8080/

<br>

### Shutdown steps

##### 10. stop Postgres and Adminer services by shutdown Docker:
```sh
$ docker-compose down
```

##### 11. deactivate the visual environment:
```sh
$ deactivate
```
