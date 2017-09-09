# Python wrapper for Google's CSE

This python program uses the Google API for the Custom Search Engine (CSE) product.
I coded this for a research assistance job.

Instead of scraping Google's search result web page, Google offers the CSE so one can customize the behavior of the engine and, optionally, programmatically retrieve search results using a RESTful API.
Google also offers a Python client library, so that any Python programmer can easily use the API.

The goal of the program is to take a list of strings to be searched using some CSE and return a database listing the URLs associated to each search.
If you plan to use this in conjunction with Google's paid plan, then note my program is capable of splitting the whole job in order to schedule 10k searches every 24 hours.
Scheduling in this sense is necessary because Google restricts the maximum number of daily searches, as well as the rate at which search requests are served.
The end product of this code is a SQLite database, which can be inspected and worked with using tools like [DB Browser for SQLite](http://sqlitebrowser.org/).

Information about Google's API can be found at its [GitHub repo](https://github.com/google/google-api-python-client), on [developers.google.com](https://developers.google.com/api-client-library/python/) and on [Python's Package Index](https://pypi.python.org/pypi/google-api-python-client/).
Obviously, the package `google-api-python-client` is a hard dependency of my code.

Please note, I did not specifically aim at computational efficiency when I coded this thing, although I tried my best to be gentle on RAM (by saving on disk) and on network usage.
Therefore, there is space for performance improvement.


## Usage

_[Coming soon]_
