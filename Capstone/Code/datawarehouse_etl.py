# pip3.9 install psycopg2-binary --force-reinstall --no-cache-dir
import psycopg2

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
copy_table_queries = [
    # """
    # COPY staging_events FROM 's3://zkan-swu-labs/github_events_01.json'
    # ACCESS_KEY_ID 'your_access_key_id'
    # SECRET_ACCESS_KEY 'your_secret_access_key'
    # SESSION_TOKEN 'your_session_token'
    # JSON 's3://zkan-swu-labs/events_json_path.json'
    # REGION 'us-east-1'
    # """,
    """
    COPY leagues 
    FROM 's3://jaochin-dataset-fifa/cleaned/leagues/date_oprt=2022-12-07'
    ACCESS_KEY_ID 'ASIAXZM22O2VTY22WUN2'
    SECRET_ACCESS_KEY 'inf6RDpkjo1c0VfRrVj+2AkamuzoKaqjhw+sGJBS'
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHu7IZUlypFbyWw57yLMAQEIFEdpk2wGBoPRLSB+BWy8QptB57xf3GAke1MlEOrPYKPgWyOLtErARd4/F8X6wJ6m+LEzM8w9dVLqj7/o3vT+l31aDuMowzqbfvlmDm6OndBxZpKiTTugSFGq529eKySX5SLW+7BDcM8F6swsVZimCAU+YBj5YcJca1CVdHb/FZOdT6yrah/UaxXklrWI4U694C1F+v4JkBzSJPGLZypE4u8Oxb/wMXRegA8H33EQz+dEl5LBZ67jPjIvnWMfh/OUNBVEEUg2Uo0GQiihmsKcBjItX3mx3Q+sq1PmSHzoNvhot/pNXyq7YhwF1IB0u/4wUDEtAfQtMCr1nqNcdWSv'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY clubs 
    FROM 's3://jaochin-dataset-fifa/cleaned/clubs/date_oprt=2022-12-07'
    ACCESS_KEY_ID 'ASIAXZM22O2VTY22WUN2'
    SECRET_ACCESS_KEY 'inf6RDpkjo1c0VfRrVj+2AkamuzoKaqjhw+sGJBS'
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHu7IZUlypFbyWw57yLMAQEIFEdpk2wGBoPRLSB+BWy8QptB57xf3GAke1MlEOrPYKPgWyOLtErARd4/F8X6wJ6m+LEzM8w9dVLqj7/o3vT+l31aDuMowzqbfvlmDm6OndBxZpKiTTugSFGq529eKySX5SLW+7BDcM8F6swsVZimCAU+YBj5YcJca1CVdHb/FZOdT6yrah/UaxXklrWI4U694C1F+v4JkBzSJPGLZypE4u8Oxb/wMXRegA8H33EQz+dEl5LBZ67jPjIvnWMfh/OUNBVEEUg2Uo0GQiihmsKcBjItX3mx3Q+sq1PmSHzoNvhot/pNXyq7YhwF1IB0u/4wUDEtAfQtMCr1nqNcdWSv'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY nationalities 
    FROM 's3://jaochin-dataset-fifa/cleaned/nationalities/date_oprt=2022-12-07'
    ACCESS_KEY_ID 'ASIAXZM22O2VTY22WUN2'
    SECRET_ACCESS_KEY 'inf6RDpkjo1c0VfRrVj+2AkamuzoKaqjhw+sGJBS'
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHu7IZUlypFbyWw57yLMAQEIFEdpk2wGBoPRLSB+BWy8QptB57xf3GAke1MlEOrPYKPgWyOLtErARd4/F8X6wJ6m+LEzM8w9dVLqj7/o3vT+l31aDuMowzqbfvlmDm6OndBxZpKiTTugSFGq529eKySX5SLW+7BDcM8F6swsVZimCAU+YBj5YcJca1CVdHb/FZOdT6yrah/UaxXklrWI4U694C1F+v4JkBzSJPGLZypE4u8Oxb/wMXRegA8H33EQz+dEl5LBZ67jPjIvnWMfh/OUNBVEEUg2Uo0GQiihmsKcBjItX3mx3Q+sq1PmSHzoNvhot/pNXyq7YhwF1IB0u/4wUDEtAfQtMCr1nqNcdWSv'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY positions 
    FROM 's3://jaochin-dataset-fifa/cleaned/positions/date_oprt=2022-12-07'
    ACCESS_KEY_ID 'ASIAXZM22O2VTY22WUN2'
    SECRET_ACCESS_KEY 'inf6RDpkjo1c0VfRrVj+2AkamuzoKaqjhw+sGJBS'
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHu7IZUlypFbyWw57yLMAQEIFEdpk2wGBoPRLSB+BWy8QptB57xf3GAke1MlEOrPYKPgWyOLtErARd4/F8X6wJ6m+LEzM8w9dVLqj7/o3vT+l31aDuMowzqbfvlmDm6OndBxZpKiTTugSFGq529eKySX5SLW+7BDcM8F6swsVZimCAU+YBj5YcJca1CVdHb/FZOdT6yrah/UaxXklrWI4U694C1F+v4JkBzSJPGLZypE4u8Oxb/wMXRegA8H33EQz+dEl5LBZ67jPjIvnWMfh/OUNBVEEUg2Uo0GQiihmsKcBjItX3mx3Q+sq1PmSHzoNvhot/pNXyq7YhwF1IB0u/4wUDEtAfQtMCr1nqNcdWSv'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY players 
    FROM 's3://jaochin-dataset-fifa/cleaned/players/date_oprt=2022-12-07'
    ACCESS_KEY_ID 'ASIAXZM22O2VTY22WUN2'
    SECRET_ACCESS_KEY 'inf6RDpkjo1c0VfRrVj+2AkamuzoKaqjhw+sGJBS'
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHu7IZUlypFbyWw57yLMAQEIFEdpk2wGBoPRLSB+BWy8QptB57xf3GAke1MlEOrPYKPgWyOLtErARd4/F8X6wJ6m+LEzM8w9dVLqj7/o3vT+l31aDuMowzqbfvlmDm6OndBxZpKiTTugSFGq529eKySX5SLW+7BDcM8F6swsVZimCAU+YBj5YcJca1CVdHb/FZOdT6yrah/UaxXklrWI4U694C1F+v4JkBzSJPGLZypE4u8Oxb/wMXRegA8H33EQz+dEl5LBZ67jPjIvnWMfh/OUNBVEEUg2Uo0GQiihmsKcBjItX3mx3Q+sq1PmSHzoNvhot/pNXyq7YhwF1IB0u/4wUDEtAfQtMCr1nqNcdWSv'
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


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def truncate_tables(cur, conn):
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.c7om6vv9mbp9.us-east-1.redshift.amazonaws.com"
    port = "5439"
    dbname = "dev"
    user = "awsuser"
    password = "awsPassword1"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    create_tables(cur, conn)
    truncate_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # query data
    query = "select * from player_value_wage"
    cur.execute(query)
    # print data
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()

    