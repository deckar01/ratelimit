import unittest
import sqlite3
from ratelimit import limits, RateLimitException

class TestDecorator(unittest.TestCase):

    def sleep(self, n):
        # Push calls back in time to simulate sleep
        self.database.execute("UPDATE main_limit SET time = julianday(time, '{} seconds')".format(-n))
        self.database.commit()

    @limits(calls=1, period=10)
    def increment(self):
        '''
        Increment the counter at most once every 10 seconds.
        '''
        self.count += 1

    @limits(calls=1, period=10, raise_on_limit=False)
    def increment_no_exception(self):
        '''
        Increment the counter at most once every 10 seconds, but w/o rasing an
        exception when reaching limit.
        '''
        self.count += 1

    def setup_method(self, _):
        self.count = 0
        self.database = sqlite3.connect('file:ratelimit?mode=memory&cache=shared', uri=True)

    def teardown_method(self, _):
        self.database.execute('DELETE FROM main_limit')
        self.database.commit()

    def test_increment(self):
        self.increment()
        self.assertEqual(self.count, 1)

    def test_exception(self):
        self.increment()
        self.assertRaises(RateLimitException, self.increment)

    def test_reset(self):
        self.increment()
        self.sleep(10)

        self.increment()
        self.assertEqual(self.count, 2)

    def test_no_exception(self):
        self.increment_no_exception()
        self.increment_no_exception()

        self.assertEqual(self.count, 1)
