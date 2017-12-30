import json
import sqlite3
import string
from functools import reduce
from multiprocess.pool import Pool
from multiprocessing import reduction
from multiprocessing import cpu_count
import sys
from my_utils.platfowm_vars import ROOTDIR, dir_sep
from my_utils.my_logging import log_message as log, set_logfile_name
from my_utils.sqlite_utils import create_connection

set_logfile_name("word_counter")

rows_per_loop = 100000

# this script counts the freq for each word in the database
def count_words(): # TODO CLEAN THE PUNCTUATION BETTER
    db_cursor = create_connection(ROOTDIR + dir_sep + "stage_2_clean.db").cursor()
    db_cursor.execute("select bodies from bodies")
    loaded_data = db_cursor.fetchmany(rows_per_loop)

    log("")
    log("start")

    data = [{}]
    i = 1
    more_data = True
    while more_data:
        log("splitting data")
        for line in loaded_data:
            line = line[0]
            line = ''.join(char for char in line if char not in string.punctuation)
            for word in line.split(" "):
                data.append(word)


        log("reducing data")
        data = [reduce(lambda x ,y: add_word_to_dict(x, y), data)]
        log("done loop " + str(i) + ", getting more data.")
        i +=1

        loaded_data = db_cursor.fetchmany(rows_per_loop)
        #more_data = False
        if len(loaded_data) < 1:
            more_data = False
        if not more_data:
            log("end of data")
    log("done")
    log("list is  " + str(len(data[0])) + " words")
    dict_to_disk(data, ROOTDIR + dir_sep + "word_count" + dir_sep + "test.json")
    print()

def add_word_to_dict(dict, word):
    if word is not '':
        if word in dict:
            dict[word] += 1
        else:
            dict[word] = 1
    return dict


def dict_to_disk(dict, filepath):
    with open(filepath, 'w', encoding="UTF-8") as file:
        json.dump(dict, file, indent=4, sort_keys=True, ensure_ascii=False)


if __name__ == '__main__':
    count_words()