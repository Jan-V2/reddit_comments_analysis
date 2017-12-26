import sqlite3


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        test = sqlite3.version
        print("connected to " + db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
