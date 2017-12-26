import sqlite3
from sqlite3 import Error
from my_utils.platfowm_vars import ROOTDIR, dir_sep
from my_utils.my_logging import log_message as log, set_logfile_name
from my_utils.sqlite_utils import create_connection

set_logfile_name("bodies_from_db_getter")



def run():
    origanal_db_cursor = create_connection(comments_db_path).cursor()
    new_db = create_connection(ROOTDIR + dir_sep + "dirty_bodies.db")
    new_db_cursor = new_db.cursor()

    new_db_cursor.execute("DELETE FROM bodies")
    new_db_cursor.execute("delete from sqlite_sequence where name='bodies'")
    origanal_db_cursor.execute("select body from May2015")
    data = origanal_db_cursor.fetchmany(rows_per_loop)
    more_data = True
    while more_data:
        query = "insert into bodies (bodies) values (?)"
        log("inserting 50k rows")
        new_db_cursor.executemany(query, data)
        new_db.commit()

        data = origanal_db_cursor.fetchmany(rows_per_loop)
        if len(data) is 0:
            more_data = False



rows_per_loop = 50000
get_comments_q = "select body from May2015"
comments_db_path = 'H:\\data\\reddit-comments-may-2015\\reddit-comments-may-2015.sqlite'

if __name__ == '__main__':
    run()