from pathlib import Path
import json
import os
import argparse
import mysql.connector

def json_to_sql(json_file, sql_file):
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return
    
    with open(sql_file, 'w') as f:
        for record in data:
            columns = ', '.join(record.keys())
            # doing some brute force string replacements
            values = ', '.join(f"'{v.replace("'", "''").replace("\", "/"").replace('\n', ' ').replace('\r', ' ').strip()}'" if isinstance(v, str) else str(v) for v in record.values())
            sql = f"INSERT INTO table_name ({columns}) VALUES ({values});\n"
            f.write(sql)


def get_connection(user, password, host, port, database):
    return mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        database=database,
        port=3307,
    )

def import_mysql(sql_file, user, password, host, port, database):

    # Create a MySQL connection
    cnx = get_connection(user, password, host, port, database)

    # Create a cursor object
    cursor = cnx.cursor()

    # Read the SQL file
    file_name = Path(sql_file).name.split('.')[0]

    with open(sql_file, 'r') as f:
        sql = f.read()
    sql = sql.replace('table_name', file_name)

    # Check if the table is empty
    cursor.execute(f"SELECT COUNT(*) FROM {file_name};")
    count = cursor.fetchone()[0]
    if count > 0:
        print(f"Table {file_name} is not empty. Aborting import.")
        os.remove(sql_file)
        cursor.close()
        cnx.close()
        return

    # Split the SQL into batches of 500 lines
    sql_commands = sql.splitlines()
    batch_size = 5000
    for i in range(0, len(sql_commands), batch_size):
        batch = "\n".join(sql_commands[i:i + batch_size])
        for result in cursor.execute(batch, multi=True):
            if result.with_rows:
                result.fetchall()

    # Commit the changes
    cnx.commit()
    cursor.close()
    cnx.close()

def generate_json_to_sql():
    sql_path = Path("sql")
    sql_path.mkdir(exist_ok=True)

    for file in Path("json").rglob("*.json"):
        # check the file is not empty
        if file.stat().st_size == 0:
            continue

        print(file)

        output_file = sql_path / f"{file.name}.sql"

        json_to_sql(str(file), str(output_file))


def execute_sql(user, password, host, port, database):
    for file in Path("sql").rglob("*.sql"):
        print(file)
        try:
            import_mysql(str(file))
        except Exception as e:
            print(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MySQL connection settings')
    parser.add_argument('--user', help='Username for the MySQL database', default='root')
    parser.add_argument('--password', help='Password for the MySQL database')
    parser.add_argument('--host', help='Host name or IP address of the MySQL server', default="127.0.0.1")
    parser.add_argument('--port', type=int, help='Port number of the MySQL server', default=3306)
    parser.add_argument('--database', help='Name of the MySQL database', default="acore_world")

    args = parser.parse_args()

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    database = args.database

    generate_json_to_sql()
    execute_sql(user, password, host, port, database)
