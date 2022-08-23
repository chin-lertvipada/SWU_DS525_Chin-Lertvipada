import psycopg2


table_drop_repo  = "DROP TABLE IF EXISTS Repo;"
table_drop_org   = "DROP TABLE IF EXISTS Org;"
table_drop_actor = "DROP TABLE IF EXISTS Actor;"
table_drop_event = "DROP TABLE IF EXISTS Event;"

table_create_repo = """
    CREATE TABLE IF NOT EXISTS Repo (
        repo_id BIGINT NOT NULL,
        repo_name VARCHAR(100) NOT NULL,
        repo_url VARCHAR(150) NOT NULL,
        PRIMARY KEY (repo_id)
    );
"""
table_create_org = """
    CREATE TABLE IF NOT EXISTS Org (
        org_id BIGINT NOT NULL,
        org_login VARCHAR(50) NOT NULL,
        org_gravatar_id VARCHAR(50),
        org_url VARCHAR(100) NOT NULL,
        org_avatar_url VARCHAR(100) NOT NULL,
        PRIMARY KEY (org_id)
    );
"""
table_create_actor = """
    CREATE TABLE IF NOT EXISTS Actor (
        actor_id BIGINT NOT NULL,
        actor_login VARCHAR(50) NOT NULL,
        actor_display_login VARCHAR(50) NOT NULL,
        actor_gravatar_id VARCHAR(50),
        actor_url VARCHAR(100) NOT NULL,
        actor_avatar_url VARCHAR(100) NOT NULL,
        PRIMARY KEY (actor_id)
    );
"""
table_create_event = """
    CREATE TABLE IF NOT EXISTS Event (
        event_id VARCHAR(20) NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        event_public BOOLEAN NOT NULL,
        event_created_at TIMESTAMP NOT NULL,
        event_repo_id BIGINT NOT NULL,
        event_actor_id BIGINT NOT NULL,
        event_org_id BIGINT,
        PRIMARY KEY (event_id),
        FOREIGN KEY (event_repo_id)  REFERENCES Repo  (repo_id),
        FOREIGN KEY (event_actor_id) REFERENCES Actor (actor_id),
        FOREIGN KEY (event_org_id)   REFERENCES Org   (org_id)
    );
"""

drop_table_queries   = [table_drop_event, table_drop_repo, table_drop_org, table_drop_actor]
create_table_queries = [table_create_repo, table_create_org, table_create_actor, table_create_event]


PostgresCursor,PostgresConn = 0,0

def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

