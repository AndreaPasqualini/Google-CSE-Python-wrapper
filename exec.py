import sys
from sqlite3 import OperationalError
from shutil import copyfile
import wmlib as wm


def main(csv_file, cseID, apiKey):
    # CONFIG ==================================================================
    filename = csv_file[:-4]
    daily_job_size = int(1e4)  # dictated by Google
    n_search_results = 10  # upper bound is 10, dictated by Google

    # PRELIMINARY OPERATIONS ==================================================
    terms = wm.load_csv(csv_file)
    storage_name = filename + '.db'
    database = wm.Storage(storage_name)
    try:
        database.create_response_table()
    except OperationalError:  # thrown by sqlite3 if table already exists
        pass

    # BODY ====================================================================
    schedule = wm.Scheduler(terms, daily_job_size)
    wm.log("Processing of file '{0}' started.".format(filename))
    wm.log("Will finish to process file '{0}' on date {1}".format(filename,
           schedule.calendar[-1].strftime("%d %b %Y")))

    for day in schedule.calendar:
        wm.log("Waiting to start job scheduled on {0}...".format(
            day.strftime("%d %b %Y")))

        wm.wait_until(23, recheck_every=30)
        wm.log("Starting new daily job now...")

        daily_chunk = schedule.daily_task(day.strftime("%Y-%m-%d"))
        job = wm.DailyJob(daily_chunk, database, day)
        job.search(n_res=n_search_results, api_key=apiKey, cse_id=cseID)
        wm.log("Data retrieval completed.")

    # FINAL OPERATIONS ========================================================
    database.save()
    wm.log("Processing of file '{0}' completed.".format(filename))
    database.create_urls_table()
    database.response_to_urls()
    wm.log("Extraction of URLs and corrected queries done.")
    database.save()
    database.close()
    copyfile(storage_name, './backups/' + filename + '_backup.db')
    wm.log("Complete database backed up.")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
