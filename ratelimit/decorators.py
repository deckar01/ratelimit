'''
Rate limit public interface.

This module includes the decorator used to rate limit function invocations.
Additionally this module includes a naive retry strategy to be used in
conjunction with the rate limit decorator.
'''
from functools import wraps
from math import floor

import time
import sys
import threading
import sqlite3

from ratelimit.exception import RateLimitException

class RateLimitDecorator(object):
    '''
    Rate limit decorator class.
    '''
    def __init__(self, calls=15, period=900, raise_on_limit=True, storage='file:ratelimit?mode=memory&cache=shared', name='main_limit'):
        '''
        Instantiate a RateLimitDecorator with some sensible defaults. By
        default the Twitter rate limiting window is respected (15 calls every
        15 minutes).

        :param int calls: Maximum function invocations allowed within a time period.
        :param float period: An upper bound time period (in seconds) before the rate limit resets.
        :param bool raise_on_limit: A boolean allowing the caller to avoiding rasing an exception.
        :param string storage: An sqlite3 database path for storing the call history.
        :param string name: The name of the sqlite3 table.
        '''
        self.clamped_calls = max(1, min(sys.maxsize, floor(calls)))
        self.period = period
        self.raise_on_limit = raise_on_limit

        self.database = sqlite3.connect(storage, uri=True, check_same_thread=False)
        self.name = name

        # Add thread safety.
        self.lock = threading.RLock()

        try:
            with self.database:
                self.database.execute(
                    """
                    CREATE TABLE {}
                    (time DATETIME DEFAULT(julianday('now')))
                    """.format(self.name)
                )
        except sqlite3.OperationalError:
            pass

    @property
    def _offset(self):
        return str(-self.period) + ' seconds'

    @property
    def _num_calls(self):
        query = self.database.execute(
            """
            SELECT count(*) FROM {}
            WHERE time > julianday('now', '{}')
            """.format(self.name, self._offset)
        )
        return int(query.fetchone()[0])

    @property
    def _period_remaining(self):
        query = self.database.execute(
            """
            SELECT julianday('now') - time FROM {}
            WHERE time > julianday('now', '{}')
            LIMIT 1
            """.format(self.name, self._offset)
        )
        result = query.fetchone()
        if result:
            oldest_age = 24*60*60*float(result[0])
            return max(0, self.period - oldest_age)
        return 0

    def __call__(self, func):
        '''
        Return a wrapped function that prevents further function invocations if
        previously called within a specified period of time.

        :param function func: The function to decorate.
        :return: Decorated function.
        :rtype: function
        '''
        @wraps(func)
        def wrapper(*args, **kargs):
            '''
            Extend the behaviour of the decorated function, forwarding function
            invocations previously called no sooner than a specified period of
            time. The decorator will raise an exception if the function cannot
            be called so the caller may implement a retry strategy such as an
            exponential backoff.

            :param args: non-keyword variable length argument list to the decorated function.
            :param kargs: keyworded variable length argument list to the decorated function.
            :raises: RateLimitException
            '''
            while True:
                try:
                    with self.database, self.lock:
                        self.database.execute("BEGIN TRANSACTION")
                        # If the number of attempts to call the function exceeds the
                        # maximum then raise an exception.
                        if self._num_calls >= self.clamped_calls:
                            if self.raise_on_limit:
                                raise RateLimitException('too many calls', self._period_remaining)
                            return
                        # Clean old calls
                        self.database.execute(
                            """
                            DELETE FROM {}
                            WHERE time <= julianday('now', '{}')
                            """.format(self.name, self._offset)
                        )
                        # Log call
                        self.database.execute("INSERT INTO {} DEFAULT VALUES".format(self.name))
                    return func(*args, **kargs)
                except sqlite3.OperationalError:
                    pass
        return wrapper

def sleep_and_retry(func):
    '''
    Return a wrapped function that rescues rate limit exceptions, sleeping the
    current thread until rate limit resets.

    :param function func: The function to decorate.
    :return: Decorated function.
    :rtype: function
    '''
    @wraps(func)
    def wrapper(*args, **kargs):
        '''
        Call the rate limited function. If the function raises a rate limit
        exception sleep for the remaing time period and retry the function.

        :param args: non-keyword variable length argument list to the decorated function.
        :param kargs: keyworded variable length argument list to the decorated function.
        '''
        while True:
            try:
                return func(*args, **kargs)
            except RateLimitException as exception:
                time.sleep(exception.period_remaining)
    return wrapper
