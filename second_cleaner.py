import json
import string
from pprint import pprint

from my_utils.platfowm_vars import ROOTDIR, dir_sep
from my_utils.my_logging import log_message as log, set_logfile_name
from my_utils.sqlite_utils import create_connection
from multiprocessing import cpu_count
from multiprocess.pool import Pool

set_logfile_name("second_cleaner")

cpus = cpu_count()


def clean_data():
    rows_per_loop = 100000
    log("")
    log("starting")

    dirty_db_path = ROOTDIR + dir_sep + "stage_2_clean.db"
    clean_db_path = ROOTDIR + dir_sep + "stage_3_cleaner.db"
    dirty_db_cursor = create_connection(dirty_db_path).cursor()
    clean_db = create_connection(clean_db_path)
    clean_db_cursor = clean_db.cursor()

    clean_db_cursor.execute("DELETE FROM bodies")
    clean_db_cursor.execute("delete from sqlite_sequence where name='bodies'")

    dirty_db_cursor.execute("select bodies from bodies")
    data = dirty_db_cursor.fetchmany(rows_per_loop)

    tpool = Pool(processes=4)
    locp_n = 1
    log("detected " + str(cpus) + " as cpu count")
    inserted = 0
    more_data = True
    while more_data:
        log("cleaning data")
        data = tpool.map(clean_line, data)
        data = list(map(lambda line: (line,), data))

        log("inserting 100k rows")
        query = "insert into bodies (bodies) values (?)"
        clean_db_cursor.executemany(query, data)
        clean_db.commit()

        log("done loop, getting more data.")
        inserted += len(data)
        data = dirty_db_cursor.fetchmany(rows_per_loop)
        #more_data = False
        if len(data) < 1:
            more_data = False
            log("end of data")
        log("done " + str(locp_n) + " loops")
        locp_n += 1
    log("done")
    log("inserted " + str(inserted) + " rows")

def clean_line(line):
    line = line[0]
    line = line.replace("\t", "").replace("\b", "").replace('', "")
    line = ''.join(char for char in line if char not in string.digits or string.punctuation)
    line = line.lower()
    return (line)

if __name__ == '__main__':
    clean_data()