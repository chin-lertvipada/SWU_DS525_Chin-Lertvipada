import psycopg2

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id bigint,
        league_name text,
        date_oprt date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS clubs (
        club_id bigint,
        club_name text,
        league_id bigint,
        date_oprt date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS nationalities (
        nationality_id bigint,
        nationality_name text,
        date_oprt date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        positions_id bigint,
        positions_name text,
        date_oprt date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS players (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value double,
        player_wage double,
        positions_id bigint,
        club_id bigint,
        nationality_id bigint,
        date_oprt date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_value_wage (
        player_id bigint,
        player_name text,
        player_age int,
        player_overall int,
        player_value double,
        player_wage double,
        positions_name text,
        club_name text,
        nationality_name text,
        league_name text,
        date_oprt date
    )
    """,
]

copy_table_queries = [
    # """
    # COPY staging_events FROM 's3://chin-swu-lab3/github_events_01.json'
    # CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    # JSON 's3://chin-swu-lab3/events_json_path.json'
    # REGION 'us-east-1'
    # """,
    """
    COPY leagues 
    FROM 's3://chin-fifa/landing/leagues/date_oprt=2022-12-05/*.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    CSV
    """,
    """
    COPY clubs 
    FROM 's3://chin-fifa/landing/clubs/date_oprt=2022-12-05/*.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    CSV
    """,
    """
    COPY nationalities 
    FROM 's3://chin-fifa/landing/nationalities/date_oprt=2022-12-05/*.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    CSV
    """,
    """
    COPY positions 
    FROM 's3://chin-fifa/landing/positions/date_oprt=2022-12-05/*.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    CSV
    """,
    """
    COPY players 
    FROM 's3://chin-fifa/landing/players/date_oprt=2022-12-05/*.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::535584994987:role/LabRole'
    CSV
    """,
]

insert_table_queries = [
    """
    INSERT INTO events ( id, type, actor, repo, created_at )
    SELECT DISTINCT id, type, actor_name, repo_name, created_at
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM events)
    """,
    """
    INSERT INTO actors ( id, name, url )
    SELECT DISTINCT actor_id, actor_name, actor_url
    FROM staging_events
    WHERE actor_id NOT IN (SELECT DISTINCT id FROM actors)
    """,
    """
    INSERT INTO repos ( id, name, url )
    SELECT DISTINCT repo_id, repo_name, repo_url
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM repos)
    """,
    """
    INSERT INTO player_value_wage 
    SELECT p.player_id
        , p.player_name
        , p.player_age 
        , p.player_overall 
        , p.player_value 
        , p.player_wage 
        , pos.positions_name 
        , c.club_name 
        , n.nationality_name 
        , l.league_name 
        , current_date()
    FROM players p
    INNER JOIN positions pos
        ON pos.date_oprt = '2022-12-05' 
       AND pos.position_id = p.position_id
    INNER JOIN nationalities n
        ON n.date_oprt = '2022-12-05' 
       AND n.nationality_id = p.nationality_id
    INNER JOIN clubs c
        ON c.date_oprt = '2022-12-05' 
       AND c.club_id = p.club_id
    INNER JOIN leagues l
        ON l.date_oprt = '2022-12-05' 
       AND l.league_id = c.league_id
    WHERE p.date_oprt = '2022-12-05'
    """,
]


# def drop_tables(cur, conn):
#     for query in drop_table_queries:
#         cur.execute(query)
#         conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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

    # drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # query data
    query = "select * from events"
    cur.execute(query)
    # print data
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()

    