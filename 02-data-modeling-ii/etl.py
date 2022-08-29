from cassandra.cluster import Cluster
import glob
import json
import os
from typing import List


table_drop_index = "DROP TABLE indexs"
table_drop_mostActActors = "DROP TABLE mostActActors"
table_drop_mostReachRepos = "DROP TABLE mostReachRepos"
# view_drop_mostReachRepos = "DROP MATERIALIZED VIEW IF EXISTS v_mostReachRepos"


table_create_index = """
    CREATE TABLE IF NOT EXISTS indexs (
        event_id              bigint,
        event_type            varchar,
        event_public          boolean,
        event_created_at      timestamp,
        actor_id              bigint,
        actor_display_login   varchar,
        actor_url             varchar,
        repo_id               bigint,
        repo_name             varchar,
        repo_url              varchar,
        org_id                bigint,
        org_login             varchar,
        org_url               varchar,
        PRIMARY KEY ((event_type), event_created_at)
    )
"""
table_create_mostActActors = """
    CREATE TABLE IF NOT EXISTS mostActActors (
        actor_display_login     varchar,
        cnt                     bigint,
        PRIMARY KEY ((actor_display_login), cnt)
    )
"""
table_create_mostReachRepos = """
    CREATE TABLE IF NOT EXISTS mostReachRepos (
        repo_name   varchar,
        cnt         bigint,
        PRIMARY KEY ((repo_name), cnt)
    )
"""
# table_create_mostReachRepos = """
#     CREATE TABLE IF NOT EXISTS mostReachRepos (
#         repo_id     bigint,
#         repo_name   varchar,
#         repo_url    varchar,
#         event_id    bigint,
#         event_type  varchar,
#         PRIMARY KEY ((repo_id), event_type)
#     )
# """
# view_create_mostReachRepos = """
#     CREATE MATERIALIZED VIEW v_mostReachRepos 
#     AS SELECT repo_name, cnt FROM mostReachRepos
#     WHERE cnt > 2 PRIMARY KEY (repo_name)
#     WITH CLUSTERING ORDER BY (cnt DESC)
# """


drop_table_queries = [
    table_drop_index, table_drop_mostActActors, table_drop_mostReachRepos
]
create_table_queries = [
    table_create_index, table_create_mostActActors, table_create_mostReachRepos
]


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        # files = glob.glob(os.path.join(root, "github_events.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def process(session, filepath):

    cnt_repo, cnt_actor = {}, {}

    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())

            for each in data:
                # Insert table_create_index
                try: 
                    query = "INSERT INTO indexs (event_id, event_type, event_public, event_created_at, actor_id, actor_display_login \
                        , actor_url, repo_id, repo_name, repo_url, org_id, org_login, org_url) \
                        VALUES (%s, '%s', %s, '%s', %s, '%s', '%s', %s, '%s', '%s', %s, '%s', '%s')" \
                        % (each["id"], each["type"], each["public"], each["created_at"], each["actor"]["id"] \
                        , each["actor"]["display_login"], each["actor"]["url"], each["repo"]["id"], each["repo"]["name"] \
                        , each["repo"]["url"], each["org"]["id"], each["org"]["login"], each["org"]["url"])
                except:
                    query = "INSERT INTO indexs (event_id, event_type, event_public, event_created_at, actor_id, actor_display_login \
                        , actor_url, repo_id, repo_name, repo_url) \
                        VALUES (%s, '%s', %s, '%s', %s, '%s', '%s', %s, '%s', '%s')" \
                        % (each["id"], each["type"], each["public"], each["created_at"], each["actor"]["id"] \
                        , each["actor"]["display_login"], each["actor"]["url"], each["repo"]["id"], each["repo"]["name"], each["repo"]["url"])
                # print(query)
                session.execute(query)

                # Insert table_create_mostActActors
                if each["actor"]["display_login"] in cnt_actor:
                    cnt_actor[each["actor"]["display_login"]].append(each["id"])
                else:
                    cnt_actor[each["actor"]["display_login"]] = [each["id"]]

                # Insert table_create_mostReachRepos
                if each["repo"]["name"] in cnt_repo:
                    cnt_repo[each["repo"]["name"]].append(each["id"])
                else:
                    cnt_repo[each["repo"]["name"]] = [each["id"]]


    # Insert table_create_mostActActors
    for k, v in cnt_actor.items():
        query = "INSERT INTO mostActActors (actor_display_login, cnt) VALUES ('%s', %s)" \
                % (k, len(v))
        session.execute(query)

    # Insert table_create_mostReachRepos
    for k, v in cnt_repo.items():
        query = "INSERT INTO mostReachRepos (repo_name, cnt) VALUES ('%s', %s)" \
                % (k, len(v))
        session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)
    process(session, filepath="../data")

    # Select data in Cassandra and print them to stdout
    query_index = """
    SELECT event_created_at, event_type, repo_name, repo_url, actor_display_login, actor_url, org_login, org_url
        FROM indexs --WHERE event_type = 'PushEvent' ORDER BY event_created_at DESC
    """
    query_mostActActors = """
    SELECT actor_display_login, cnt FROM mostActActors WHERE cnt > 2 ALLOW FILTERING
    """
    query_mostReachRepos = """
    SELECT repo_name, cnt FROM mostReachRepos WHERE cnt > 2 ALLOW FILTERING
    """
    # query_mostReachRepos = """
    # SELECT * FROM v_mostReachRepos
    # """

    try:
        rows = session.execute(query_index)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()