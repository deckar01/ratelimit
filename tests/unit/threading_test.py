from threading import Thread
from queue import Queue
import unittest
import sqlite3
from ratelimit import limits, RateLimitException


@limits(calls=2, period=10)
def decorate_in_shared(q):
    '''
    Increment the counter at most once every 10 seconds.
    '''
    q.put(1)

def decorate_in_thread(q):
    '''
    Increment the counter at most once every 10 seconds.
    '''
    @limits(calls=2, period=10)
    def run():
        q.put(1)
    run()

class TestPreThreading(unittest.TestCase):

    def setup_method(self, _):
        self.shared = {'count': 0}
        self.queue = Queue()
        self.threads = [Thread(target=decorate_in_shared, args=(self.queue,)) for _ in range(2)]

    def teardown_method(self, _):
        database = sqlite3.connect('file:ratelimit?mode=memory&cache=shared', uri=True)
        database.execute('DELETE FROM main_limit')
        database.commit()
        database.close()

    def test_increment(self):
        self.threads[0].start()
        self.threads[1].start()
        self.threads[0].join()
        self.threads[1].join()
        self.assertEqual(self.queue.qsize(), 2)

    def test_exception(self):
        self.threads[0].start()
        self.threads[1].start()
        self.threads[0].join()
        self.threads[1].join()
        database = sqlite3.connect('file:ratelimit?mode=memory&cache=shared', uri=True,)
        a = database.execute('SELECT time from main_limit').fetchall()
        print(a)
        self.assertRaises(RateLimitException, decorate_in_shared, q=self.queue)

class TestPostThreading(unittest.TestCase):

    def setup_method(self, _):
        self.shared = {'count': 0}
        self.queue = Queue()
        self.threads = [Thread(target=decorate_in_thread, args=(self.queue,)) for _ in range(2)]

    def teardown_method(self, _):
        database = sqlite3.connect('file:ratelimit?mode=memory&cache=shared', uri=True)
        database.execute('DELETE FROM main_limit')
        database.commit()
        database.close()

    def test_increment(self):
        self.threads[0].start()
        self.threads[1].start()
        self.threads[0].join()
        self.threads[1].join()
        self.assertEqual(self.queue.qsize(), 2)

    def test_exception(self):
        self.threads[0].start()
        self.threads[1].start()
        self.threads[0].join()
        self.threads[1].join()
        database = sqlite3.connect('file:ratelimit?mode=memory&cache=shared', uri=True,)
        a = database.execute('SELECT time from main_limit').fetchall()
        print(a)
        self.assertRaises(RateLimitException, decorate_in_thread, q=self.queue)
