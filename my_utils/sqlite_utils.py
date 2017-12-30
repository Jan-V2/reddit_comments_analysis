import sqlite3
from multiprocess.pool import Pool
import asyncio
from my_utils.my_logging import set_logfile_name, log_message as log

set_logfile_name("sqlite utils")

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        print("connected to " + db_file)
        return conn
    except sqlite3.Error as e:
        print(e)



class DB_Getter:
    # todo make this a generator?
    def __init__(self, db_path, query, max_fetch):
        self.cursor = create_connection(db_path).cursor()
        self.cursor.execute(query)
        self.maxfetch = max_fetch

    def get(self):
        ret = self.cursor.fetchmany(self.maxfetch)
        if len(ret) < 1:
            return None
        return ret

    async def get_async(self):
        return self.get()

class DB_Putter:
    def __init__(self, db_path, query):
        self.db = create_connection(db_path)
        self.cursor = self.db.cursor()
        self.query = query

    def put(self, data):
        self.cursor.executemany(self.query, data)
        self.db.commit()

    async def put_async(self, data):
        self.put(data)

class DB_Filter:

    def __init__(self, db_from_path, db_to_path, pull_query, put_query, max_fetch, filter):
        self.db_getter = DB_Getter(db_from_path, pull_query, max_fetch)
        self.db_putter = DB_Putter(db_to_path, put_query)
        self.filter = filter
        self.input_q = asyncio.Queue(3)
        self.output_q = asyncio.Queue(3)

    def perform_filter(self):
        var1 =  self.getter_loop()
        var2 =  self.filter_loop()
        ver3 =  self.putter_loop()
        await var1

    async def getter_loop(self):
        pass

    async def filter_loop(self):
        pass

    async def putter_loop(self):
        pass