import os
import glob
from sqlite3 import Timestamp
from typing import List
import json
from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook

curr_date = datetime.today().strftime('%Y-%m-%d')


create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id bigint,
        league_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS clubs (
        club_id bigint,
        club_name text,
        league_id bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS nationalities (
        nationality_id bigint,
        nationality_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id bigint,
        position_name text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS players (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value decimal,
        player_wage decimal,
        position_id bigint,
        nationality_id bigint,
        club_id bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_value_wage (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value decimal,
        player_wage decimal,
        position_name text,
        club_name text,
        nationality_name text,
        league_name text,
        date_oprt date
    )
    """,
]

truncate_table_queries = [
    """
    truncate table leagues
    """,
    """
    truncate table clubs
    """,
    """
    truncate table nationalities
    """,
    """
    truncate table positions
    """,
    """
    truncate table players
    """,
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift
access_key_id = 'ASIAXZM22O2VTZXH3EUP'
secret_access_key = '9NwRlaistTkv5qfKIdOLVN7HN6gaKB7RLux2PLnR'
session_token = 'FwoGZXIvYXdzEO3//////////wEaDNG/m1Hu7o2WAQKniiLMAf0cWfA7AvIaERqfk3MavZ8nPJAsglvjaddRra65NP/ZpVp0225ElmUQ6MgOXxIE/wW1vH18Rf+0Wa1cp2MKAnL8GVkEH/5/t0UC8pj0GX0lRXh00WbE7mB6PbwUWhEtNbRrDtlvqys5SJfEO0T0yLoJJug3/aEMJWT462P7dB0VlYczCkFyr2xrbQStfQGLVWypcKDrOX0Q65N7S86/+DMZyM3emhUGPP9sXLEkl8eUE4IYc+3d+6KOdk8J5l3w281er4JXtpetm+6t4ijmrPGcBjItdmeSaVJ0x0fasqLnmRzMrtCNBqzQMJJ6e21kq74/cUvNQC+ejl0Mwaq/igEP'

copy_table_queries = [
    """
    COPY leagues 
    FROM 's3://jaochin-dataset-fifa/cleaned/leagues/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY clubs 
    FROM 's3://jaochin-dataset-fifa/cleaned/clubs/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY nationalities 
    FROM 's3://jaochin-dataset-fifa/cleaned/nationalities/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY positions 
    FROM 's3://jaochin-dataset-fifa/cleaned/positions/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY players 
    FROM 's3://jaochin-dataset-fifa/cleaned/players/date_oprt={0}'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
]

insert_table_queries = [
    """
    INSERT INTO player_value_wage 
    SELECT p.player_id
        , p.player_name
        , p.player_age 
        , p.player_overall 
        , p.player_value 
        , p.player_wage 
        , pos.position_name 
        , c.club_name 
        , n.nationality_name 
        , l.league_name 
        , current_date
    FROM players p
    INNER JOIN positions pos
        ON pos.position_id = p.position_id
    INNER JOIN nationalities n
        ON n.nationality_id = p.nationality_id
    INNER JOIN clubs c
        ON c.club_id = p.club_id
    INNER JOIN leagues l
        ON l.league_id = c.league_id
    """,
]

host = "redshift-cluster-1.c7om6vv9mbp9.us-east-1.redshift.amazonaws.com"
port = "5439"
dbname = "dev"
user = "awsuser"
password = "awsPassword1"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _truncate_tables():
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()


def _load_staging_tables():
    for query in copy_table_queries:
        cur.execute(query.format(curr_date, access_key_id, secret_access_key, session_token))
        conn.commit()


def _insert_tables():
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


with DAG(
    'Capstone',
    start_date = timezone.datetime(2022, 12, 1),
    schedule = '@daily',
    tags = ['capstone'],
    catchup = False,
) as dag:


    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = _create_tables,
    )

    truncate_tables = PythonOperator(
        task_id = 'truncate_tables',
        python_callable = _truncate_tables,
    )

    load_staging_tables = PythonOperator(
        task_id = 'load_staging_tables',
        python_callable = _load_staging_tables,
    )

    insert_tables = PythonOperator(
        task_id = 'insert_tables',
        python_callable = _insert_tables,
    )

    create_tables >> truncate_tables >> load_staging_tables >> insert_tables