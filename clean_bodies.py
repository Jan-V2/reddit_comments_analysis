import sqlite3
from functools import reduce
from multiprocess.pool import Pool
import sys
from my_utils.platfowm_vars import ROOTDIR, dir_sep
from my_utils.my_logging import log_message as log, set_logfile_name
from my_utils.sqlite_utils import create_connection
from langdetect import detect as lang_detect, lang_detect_exception


set_logfile_name("cleaner")

rows_per_loop = 1000000


def clean_data(dirty_db_path, clean_db_path):
    dirty_db_cursor = create_connection(dirty_db_path).cursor()
    clean_db = create_connection(clean_db_path)
    clean_db_cursor = clean_db.cursor()

    clean_db_cursor.execute("DELETE FROM bodies")
    clean_db_cursor.execute("delete from sqlite_sequence where name='bodies'")

    dirty_db_cursor.execute("select bodies from bodies")
    data = dirty_db_cursor.fetchmany(rows_per_loop)

    log("start")
    inserted = 0
    more_data = True
    while more_data:
        log("cleaning data")
        data = list(map(lambda i: i[0].replace("\n", " "), data))
        data = Filter.filter(data)
        data = list(map(lambda line: (line,), data))

        log("inserting 1 million rows")
        query = "insert into bodies (bodies) values (?)"
        clean_db_cursor.executemany(query, data)
        clean_db.commit()

        log("done loop, getting more data.")
        inserted += len(data)
        data = dirty_db_cursor.fetchmany(rows_per_loop)
        if len(data) > 1:
            more_data = False
    log("done")
    log("inserted " + str(inserted) + " rows")


def filter_punctuation(comment):
    # is done last so in doesn't confuse the spellchecker
    # todo needs some tweaking maybe.
    # not escaping ' right now
    # maybe add special case for / because subreddits
    punctuation = ('(', ')', '[', ']', ",", '.', '\"', '!', '?', "/", "\\", '-', '_', '{', '}')
    replace_char = " "
    for char in punctuation:
        comment = comment.replace(char, replace_char)
    return comment

class Filter:
    @staticmethod
    def filter(dirty_data):
        log("starting filter")
        tpool = Pool(processes=16)
        ret = []
        log("filtering deleted and not english")
        for line in tpool.map(Filter.__is_not_deleted_or_not_non_english, dirty_data):
            if line[1]:
                ret.append(line[0])

        def clean_links_and_punctuation(comment):
            words = comment.split(" ")
            words = list(map(Filter.__filter_links, words))
            comment = reduce(lambda x, y: x + " " + y, words)
            return comment

        log("filtering links and punctuation")
        ret = tpool.map(clean_links_and_punctuation, ret)
        tpool.close()
        log("filter done")
        return ret


    @staticmethod
    def __is_not_deleted_or_not_non_english(comment):
        if comment is not "[deleted]":
            try:
                lang = lang_detect(comment)
            except lang_detect_exception.LangDetectException:
                lang = "  "
            if lang[0] is 'e' and lang[1] is 'n':# for some reason normal string matching did not work
                return [comment, True]
        return [comment, False]

    @staticmethod
    def __filter_links(word):
        dot_idx = word.find(".")
        if dot_idx is not -1:
            if word.rfind("/") > dot_idx or word.find("www") is not -1:
                return "POSTEDLINK"
        return word


if __name__ == '__main__':
    arguments = sys.argv[1:]
    if len(arguments) < 2:
        dirty_db_path = ROOTDIR + dir_sep + "stage_1_dirty.db"
        clean_db_path = ROOTDIR + dir_sep + "stage_2_clean.db"
    else:
        dirty_db_path = arguments[0]
        clean_db_path = arguments[1]
    clean_data(dirty_db_path, clean_db_path)