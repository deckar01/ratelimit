ratelimit |build| |maintainability|
===================================

This project is a fork of `tomasbasham/ratelimit <https://github.com/tomasbasham/ratelimit>`_
that implements a `sliding log <https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm/>`_
for correctness and provides persistance via sqlite. See the usage section on
`Persistence <#persistence>`_ for more details. Turning on persistence is highly
recommended, especially during development, to ensure rate limits are respected
between application restarts.

APIs are a very common way to interact with web services. As the need to
consume data grows, so does the number of API calls necessary to remain up to
date with data sources. However many API providers constrain developers from
making too many API calls. This is know as rate limiting and in a worst case
scenario your application can be banned from making further API calls if it
abuses these limits.

This packages introduces a function decorator preventing a function from being
called more often than that allowed by the API provider. This should prevent
API providers from banning your applications by conforming to their rate
limits.

Installation
------------

PyPi
~~~~

Add this line to your application's requirements.txt:

.. code:: python

    deckar01-ratelimit

And then execute:

.. code:: bash

    $ pip install -r requirements.txt

Or install it yourself:

.. code:: bash

    $ pip install deckar01-ratelimit

GitHub
~~~~~~

Installing the latest version from Github:

.. code:: bash

    $ git clone https://github.com/deckar01/ratelimit
    $ cd ratelimit
    $ python setup.py install

Usage
-----

To use this package simply decorate any function that makes an API call:

.. code:: python

    from ratelimit import limits

    import requests

    FIFTEEN_MINUTES = 900

    @limits(calls=15, period=FIFTEEN_MINUTES)
    def call_api(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response

This function will not be able to make more then 15 API call within a 15 minute
time period.

The arguments passed into the decorator describe the number of function
invocation allowed over a specified time period (in seconds). If no time period
is specified then it defaults to 15 minutes (the time window imposed by
Twitter).

If a decorated function is called more times than that allowed within the
specified time period then a ``ratelimit.RateLimitException`` is raised. This
may be used to implement a retry strategy such as an `expoential backoff
<https://pypi.org/project/backoff/>`_

.. code:: python

    from ratelimit import limits, RateLimitException
    from backoff import on_exception, expo

    import requests

    FIFTEEN_MINUTES = 900

    @on_exception(expo, RateLimitException, max_tries=8)
    @limits(calls=15, period=FIFTEEN_MINUTES)
    def call_api(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response

Alternatively to cause the current thread to sleep until the specified time
period has ellapsed and then retry the function use the ``sleep_and_retry``
decorator. This ensures that every function invocation is successful at the
cost of halting the thread.

.. code:: python

    from ratelimit import limits, sleep_and_retry

    import requests

    FIFTEEN_MINUTES = 900

    @sleep_and_retry
    @limits(calls=15, period=FIFTEEN_MINUTES)
    def call_api(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response

Persistence
~~~~~~~~~~~

If a limit needs to be respected between application restarts or shared by
multiple processes, the ``storage`` argument can be used to save the limit
state to disk and load it automatically.

.. code:: python

    from ratelimit import limits, sleep_a_retry

    import requests

    FIFTEEN_MINUTES = 900

    @sleep_and_retry
    @limits(calls=15, period=FIFTEEN_MINUTES, storage='ratelimit.db')
    def call_api(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response

If multiple limits need to be persisted, the ``name`` argument can be used to
store them in the same database using different tables.

.. code:: python

    from ratelimit import limits, sleep_a_retry

    import requests

    HOUR = 3600
    DAY = 24*HOUR

    @sleep_and_retry
    @limits(calls=15, period=HOUR, storage='ratelimit.db', name='hourly_limit')
    @sleep_and_retry
    @limits(calls=100, period=DAY, storage='ratelimit.db', name='daily_limit')
    def call_api(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        return response

License
-------

This project is licensed under the `MIT License <LICENSE.txt>`_.

.. |build| image:: https://travis-ci.com/deckar01/ratelimit.svg?branch=master
    :target: https://travis-ci.com/deckar01/ratelimit

.. |maintainability| image:: https://api.codeclimate.com/v1/badges/8bf92a976a1763a93339/maintainability
    :target: https://codeclimate.com/github/deckar01/ratelimit/maintainability
    :alt: Maintainability
