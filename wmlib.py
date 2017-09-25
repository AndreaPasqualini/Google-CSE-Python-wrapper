""" ===========================================================================
Author: Andrea Pasqualini
        Bocconi University, Milan
Created on: June 25, 2017
Project: Web-Matching algorithm leveraging Google CSE

This file contains the functions that constitute the library.

Note: this library depends on the module 'google-api-python-client',
which can only be downloaded via pip and not by Anaconda's package manager.
=========================================================================== """

import datetime as dt
import time
import sqlite3
import json
import csv
from googleapiclient.discovery import build
from googleapiclient import ConnectionAbortedError


def log(message, file="info.log"):
    now = dt.datetime.now().strftime("%d %b %Y, %H:%M:%S")
    string = now + " >>> " + message
    with open(file, mode='a') as f:
        f.write(string + '\n')
        f.close()
    print(string)


def load_csv(csv_file, column=0):
    with open(csv_file) as file:
        rows = csv.reader(file)
        strings = []
        for row in rows:
            strings.append(row[column])
    return strings[1:]                  # stripping the column header


def wait_until(hour, recheck_every=30):
    """
    'hour' must be an int included between 0 and 23.
    'recheck_every' must be an int and must express a number of minutes and
    must be included between 1 and 60.
    """
    if not isinstance(hour, int) or not isinstance(recheck_every, int):
        raise TypeError('Input arguments must be integers')
    if not 0 <= hour < 24:
        raise ValueError("Input 'hour' must be between 0 and 23")
    if not 1 <= recheck_every <= 60:
        raise ValueError("Input 'recheck_every' must be between 1 and 60")

    while True:
        if dt.datetime.now().hour is hour:
            break
        else:
            time.sleep(60 * recheck_every)  # check again in this many seconds
    return


class LengthError(Exception):
    pass


class GoogleAPIError(Exception):
    pass


class Scheduler:
    def __init__(self, universe, daily_length, start_date=None):
        if not isinstance(universe, list):
            raise TypeError('Universe must be a list of strings')
        if not all(isinstance(s, str) for s in universe):
            raise TypeError('Universe must only consist of strings')
        self._universe = universe
        self._daily_length = daily_length
        self._split = [self._universe[i: i+daily_length]
                       for i in range(0, len(self._universe),
                                      self._daily_length)]
        self._epoch = dt.date.today() if start_date is None else start_date
        self._days_required = - (- len(universe) // daily_length)  # ceil div
        self.calendar = [self._epoch + dt.timedelta(days=x)
                         for x in range(self._days_required)]
        self._str_calendar = [self.calendar[i].strftime('%Y-%m-%d')
                              for i in range(len(self.calendar))]
        self._schedule = dict(zip(self._str_calendar, self._split))

    def daily_task(self, today):
        """ today must be a string in the format 'YYYY-MM-DD' """
        return self._schedule[today]


class Storage:
    def __init__(self, filename,):
        self._name = filename
        self.conn = sqlite3.connect(filename)
        self.curs = self.conn.cursor()

    def save(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def create_response_table(self):
        self.curs.execute('CREATE TABLE responses'
                          '(time, id, term, status, response, exception)')
        self.save()

    def create_urls_table(self):
        self.curs.execute('CREATE TABLE urls'
                          '(time, id, term, correctedTerm, urls, notes)')
        self.save()

    def write_response_row(self, row):
        """ 'row' must be a tuple that contains entries for the fields
        (id, term, status, response, exception) """
        if not isinstance(row, tuple):
            raise TypeError('Row to be inserted must be a tuple')
        if len(row) is not 5:
            raise ValueError('Row to be inserted must have 5 entries')
        data = list(row)
        data.insert(0, dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        data = tuple(data)
        self.curs.execute('INSERT INTO responses VALUES (?,?,?,?,?,?)', data)

    def response_to_urls(self):
        rows = self.curs.execute('SELECT * FROM responses WHERE status=?', '0')
        data = []
        for row in rows:
            # need to construct a list in the format (universe, id, term, urls)
            google_response = json.loads(row[4])
            if 'items' in google_response:
                num_urls = len(google_response['items'])
                urls = [google_response['items'][i]['link']
                        for i in range(num_urls)]
                urls_str = '; '.join(urls)
                note = None
            else:
                urls_str = None
                if google_response['searchInformation']['totalResults'] is '0':
                    note = 'No search results for this term'
                else:
                    note = "Unknown occurrence: 'totalResults' is not zero," \
                           "but no key 'items' in response"

            if 'spelling' in google_response:
                correct_term = google_response['spelling']['correctedQuery']
            else:
                correct_term = None
            data.append((row[0], row[1], row[2], correct_term, urls_str, note))
        self.curs.executemany('INSERT INTO urls VALUES (?,?,?,?,?,?)', data)
        self.save()


class DailyJob:
    """ 'database' must be a Storage object """
    def __init__(self, search_terms, database, day):
        max_daily_searches = int(1e4)  # upper bound imposed by Google
        if len(search_terms) > max_daily_searches:
            raise LengthError('List of search terms has too many elements' +
                              '\n' +
                              'No. of search terms: ' + str(len(search_terms)) +
                              '\n' +
                              'Max allowed length: ' + str(max_daily_searches))
        self._db = database
        self._terms = search_terms
        self._day = day

    def _response_handler(self, id, response, exception):
        """ Elaborates answer and writes to disk. """
        status = 0 if exception is None else 1
        if response is not None:
            response = json.dumps(response)
        if exception is not None:
            exception = str(exception)
        write_out = (id, self._terms[int(id)], status, response, exception)
        self._db.write_response_row(write_out)

    def search(self, n_res=10, api_key=None, cse_id=None):
        if api_key is None or cse_id is None:
            raise GoogleAPIError('api_key and cse_id must be provided')
        if not isinstance(api_key, str) or not isinstance(cse_id, str):
            raise TypeError('api_key and cse_id must be strings')

        google = build('customsearch', 'v1', developerKey=api_key)

        max_api_calls_http = 100  # upper bound imposed by Google
        chunks = [self._terms[i: i+max_api_calls_http]
                  for i in range(0, len(self._terms), max_api_calls_http)]

        index = 0
        for n_chunk, chunk in enumerate(chunks):
            print('Chunk ' + str(n_chunk+1) + ' out of ' + str(len(chunks)))
            batch = google.new_batch_http_request()
            for term in chunk:
                batch.add(google.cse().list(q=term, num=n_res, cx=cse_id),
                          callback=self._response_handler,
                          request_id=str(index))
                index += 1
            fin = False
            while not fin:
                try:
                    batch.execute()
                    fin = True
                except ConnectionAbortedError:
                    log("ConnectionAbortedError occurred, retrying in 1 min.")
                    time.sleep(60)
            self._db.save()
            time.sleep(100)  # upper bound on response rate imposed by Google


# TODO Matcher
# class Matcher:
#     def __init__(self, universe1, universe2):
#         self._universe1 = universe1
#         self._universe2 = universe2
#
#     def compare(self, urls1, urls2):
#         set1 = set(urls1)
#         set2 = set(urls2)
#         common_elements = set1.intersection(set2)
#         how_many = len(common_elements)
#         return how_many
