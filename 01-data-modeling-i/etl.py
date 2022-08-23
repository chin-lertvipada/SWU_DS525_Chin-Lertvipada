#etl.py
import glob
import json
import os
from typing import List

import psycopg2


table_insert_repo = """
    INSERT INTO Repo VALUES %s
    ON CONFLICT DO NOTHING
"""


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                # print(each["id"], each["type"], each["actor"]["login"])
                val = (each["repo"]["id"]), (each["repo"]["name"]), (each["repo"]["url"])
                sql_insert = table_insert_repo % str(val)
                # print(sql_insert)

                # Insert data into tables here
                cur.execute(sql_insert)

                conn.commit()

def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data/")

    conn.close()


if __name__ == "__main__":
    main()
