#!/usr/bin/python

"""Test the classic PyGreSQL interface.

Sub-tests for the notification handler object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.
"""

import unittest
from threading import Thread
from time import sleep

import pg  # the module under test

from .config import dbhost, dbname, dbpasswd, dbport, dbuser

debug = False  # let DB wrapper print debugging output


def DB():  # noqa: N802
    """Create a DB wrapper object connecting to the test database."""
    db = pg.DB(dbname, dbhost, dbport, user=dbuser, passwd=dbpasswd)
    if debug:
        db.debug = debug
    return db


class TestSyncNotification(unittest.TestCase):
    """Test notification handler running in the same thread."""

    def setUp(self):
        self.db = DB()
        self.timeout = None
        self.called = True
        self.payloads = []

    def tearDown(self):
        if self.db:
            self.db.close()

    def callback(self, arg_dict):
        if arg_dict is None:
            self.timeout = True
        else:
            self.timeout = False
            self.payloads.append(arg_dict.get('extra'))

    def get_handler(self, event=None, arg_dict=None, stop_event=None):
        if not event:
            event = 'test_async_notification'
            if not stop_event:
                stop_event = 'stop_async_notification'
        callback = self.callback
        handler = self.db.notification_handler(
            event, callback, arg_dict, 0, stop_event)
        self.assertEqual(handler.event, event)
        self.assertEqual(handler.stop_event, stop_event or f'stop_{event}')
        self.assertIs(handler.callback, callback)
        if arg_dict is None:
            self.assertEqual(handler.arg_dict, {})
        else:
            self.assertIs(handler.arg_dict, arg_dict)
        self.assertEqual(handler.timeout, 0)
        self.assertFalse(handler.listening)
        return handler

    def test_close_handler(self):
        handler = self.get_handler()
        self.assertIs(handler.db, self.db)
        handler.close()
        self.assertRaises(pg.InternalError, self.db.close)
        self.db = None
        self.assertIs(handler.db, None)

    def test_delete_handler(self):
        handler = self.get_handler('test_del')
        self.assertIs(handler.db, self.db)
        handler.listen()
        self.db.query('notify test_del')
        self.db.query('notify test_del')
        del handler
        self.db.query('notify test_del')
        n = 0
        while self.db.getnotify() and n < 4:
            n += 1
        self.assertEqual(n, 2)

    def test_notify(self):
        handler = self.get_handler()
        handler.listen()
        self.assertRaises(TypeError, handler.notify, invalid=True)
        handler.notify(payload='baz')
        handler.notify(stop=True, payload='buz')
        handler.unlisten()
        self.db.close()
        self.db = None

    def test_notify_with_args_and_payload(self):
        arg_dict = {'foo': 'bar'}
        handler = self.get_handler(arg_dict=arg_dict)
        self.assertEqual(handler.timeout, 0)
        handler.listen()
        handler.notify(payload='baz')
        handler.notify(payload='biz')
        handler()
        self.assertIsNotNone(self.timeout)
        self.assertFalse(self.timeout)
        self.assertEqual(self.payloads, ['baz', 'biz'])
        self.assertEqual(arg_dict['foo'], 'bar')
        self.assertEqual(arg_dict['event'], handler.event)
        self.assertIsInstance(arg_dict['pid'], int)
        self.assertEqual(arg_dict['extra'], 'biz')
        self.assertTrue(handler.listening)
        del self.payloads[:]
        handler.notify(stop=True, payload='buz')
        handler()
        self.assertIsNotNone(self.timeout)
        self.assertFalse(self.timeout)
        self.assertEqual(self.payloads, ['buz'])
        self.assertEqual(arg_dict['foo'], 'bar')
        self.assertEqual(arg_dict['event'], handler.stop_event)
        self.assertIsInstance(arg_dict['pid'], int)
        self.assertEqual(arg_dict['extra'], 'buz')
        self.assertFalse(handler.listening)
        handler.unlisten()

    def test_notify_wrong_event(self):
        handler = self.get_handler('good_event')
        self.assertEqual(handler.timeout, 0)
        handler.listen()
        handler.notify(payload="note 1")
        self.db.query("notify bad_event, 'note 2'")
        handler.notify(payload="note 3")
        handler()
        self.assertIsNotNone(self.timeout)
        self.assertFalse(self.timeout)
        self.assertEqual(self.payloads, ['note 1', 'note 3'])
        self.assertTrue(handler.listening)
        del self.payloads[:]
        self.db.query('listen bad_event')
        handler.notify(payload="note 4")
        self.db.query("notify bad_event, 'note 5'")
        handler.notify(payload="note 6")
        try:
            handler()
        except pg.DatabaseError as error:
            self.assertEqual(
                str(error),
                'Listening for "good_event" and "stop_good_event",'
                ' but notified of "bad_event"')
        self.assertIsNotNone(self.timeout)
        self.assertFalse(self.timeout)
        self.assertEqual(self.payloads, ['note 4'])
        self.assertFalse(handler.listening)


class TestAsyncNotification(unittest.TestCase):
    """Test notification handler running in a separate thread."""

    def setUp(self):
        self.db = DB()

    def tearDown(self):
        self.doCleanups()
        if self.db:
            self.db.close()

    def callback(self, arg_dict):
        if arg_dict is None:
            self.timeout = True
        elif arg_dict is self.arg_dict:
            arg_dict = arg_dict.copy()
            pid = arg_dict.get('pid')
            if isinstance(pid, int):
                arg_dict['pid'] = 1
            self.received.append(arg_dict)
        else:
            self.received.append(dict(error=arg_dict))

    def start_handler(self, event=None, arg_dict=None,
                      timeout=5, stop_event=None):
        db = DB()
        if not event:
            event = 'test_async_notification'
            if not stop_event:
                stop_event = 'stop_async_notification'
        callback = self.callback
        handler = db.notification_handler(
            event, callback, arg_dict, timeout, stop_event)
        self.handler = handler
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertEqual(handler.event, event)
        self.assertEqual(handler.stop_event, stop_event or f'stop_{event}')
        self.event = handler.event
        self.assertIs(handler.callback, callback)
        if arg_dict is None:
            self.assertEqual(handler.arg_dict, {})
        else:
            self.assertIsInstance(handler.arg_dict, dict)
        self.arg_dict = handler.arg_dict
        self.assertEqual(handler.timeout, timeout)
        self.assertFalse(handler.listening)
        thread = Thread(target=handler, name='test_notification_thread')
        self.thread = thread
        thread.start()
        self.stopped = timeout == 0
        self.addCleanup(self.stop_handler)
        for _n in range(500):
            if handler.listening:
                break
            sleep(0.01)
        self.assertTrue(handler.listening)
        if not self.stopped:
            self.assertTrue(thread.is_alive())
        self.timeout = False
        self.received = []
        self.sent = []

    def stop_handler(self):
        handler = self.handler
        thread = self.thread
        if not self.stopped and self.handler.listening:
            self.notify_handler(stop=True)
        handler.close()
        self.db = None
        if thread.is_alive():
            thread.join(5)
        self.assertFalse(handler.listening)
        self.assertFalse(thread.is_alive())

    def notify_handler(self, stop=False, payload=None):
        event = self.event
        if stop:
            event = self.handler.stop_event
            self.stopped = True
        arg_dict = self.arg_dict.copy()
        arg_dict.update(event=event, pid=1, extra=payload or '')
        self.handler.notify(db=self.db, stop=stop, payload=payload)
        self.sent.append(arg_dict)

    def notify_query(self, stop=False, payload=None):
        event = self.event
        if stop:
            event = self.handler.stop_event
            self.stopped = True
        q = f'notify "{event}"'
        if payload:
            q += f", '{payload}'"
        arg_dict = self.arg_dict.copy()
        arg_dict.update(event=event, pid=1, extra=payload or '')
        self.db.query(q)
        self.sent.append(arg_dict)

    def wait(self):
        for _n in range(500):
            if self.timeout:
                return False
            if len(self.received) >= len(self.sent):
                return True
            sleep(0.01)

    def receive(self, stop=False):
        if not self.sent:
            stop = True
        if stop:
            self.notify_handler(stop=True, payload='stop')
        self.assertTrue(self.wait())
        self.assertFalse(self.timeout)
        self.assertEqual(self.received, self.sent)
        self.received = []
        self.sent = []
        self.assertEqual(self.handler.listening, not self.stopped)

    def test_notify_handler_empty(self):
        self.start_handler()
        self.notify_handler(stop=True)
        self.assertEqual(len(self.sent), 1)
        self.receive()

    def test_notify_query_empty(self):
        self.start_handler()
        self.notify_query(stop=True)
        self.assertEqual(len(self.sent), 1)
        self.receive()

    def test_notify_handler_once(self):
        self.start_handler()
        self.notify_handler()
        self.assertEqual(len(self.sent), 1)
        self.receive()
        self.receive(stop=True)

    def test_notify_query_once(self):
        self.start_handler()
        self.notify_query()
        self.receive()
        self.notify_query(stop=True)
        self.receive()

    def test_notify_with_args(self):
        arg_dict = {'test': 42, 'more': 43, 'less': 41}
        self.start_handler('test_args', arg_dict)
        self.notify_query()
        self.receive(stop=True)

    def test_notify_several_times(self):
        arg_dict = {'test': 1}
        self.start_handler(arg_dict=arg_dict)
        for _n in range(3):
            self.notify_query()
        self.receive()
        arg_dict['test'] += 1
        for _n in range(2):
            self.notify_handler()
        self.receive()
        arg_dict['test'] += 1
        for _n in range(3):
            self.notify_query()
        self.receive(stop=True)

    def test_notify_once_with_payload(self):
        self.start_handler()
        self.notify_query(payload='test_payload')
        self.receive(stop=True)

    def test_notify_with_args_and_payload(self):
        self.start_handler(arg_dict={'foo': 'bar'})
        self.notify_query(payload='baz')
        self.receive(stop=True)

    def test_notify_quoted_names(self):
        self.start_handler('Hello, World!')
        self.notify_query(payload='How do you do?')
        self.receive(stop=True)

    def test_notify_with_five_payloads(self):
        self.start_handler('gimme_5', {'test': 'Gimme 5'})
        for n in range(5):
            self.notify_query(payload=f"Round {n}")
        self.assertEqual(len(self.sent), 5)
        self.receive(stop=True)

    def test_receive_immediately(self):
        self.start_handler('immediate', {'test': 'immediate'})
        for n in range(3):
            self.notify_query(payload=f"Round {n}")
            self.receive()
        self.receive(stop=True)

    def test_notify_distinct_in_transaction(self):
        self.start_handler('test_transaction', {'transaction': True})
        self.db.begin()
        for n in range(3):
            self.notify_query(payload=f'Round {n}')
        self.db.commit()
        self.receive(stop=True)

    def test_notify_same_in_transaction(self):
        self.start_handler('test_transaction', {'transaction': True})
        self.db.begin()
        for _n in range(3):
            self.notify_query()
        self.db.commit()
        # these same notifications may be delivered as one,
        # so we must not wait for all three to appear
        self.sent = self.sent[:1]
        self.receive(stop=True)

    def test_notify_no_timeout(self):
        # noinspection PyTypeChecker
        self.start_handler(timeout=None)
        self.assertIsNone(self.handler.timeout)
        self.assertTrue(self.handler.listening)
        sleep(0.02)
        self.assertFalse(self.timeout)
        self.receive(stop=True)

    def test_notify_zero_timeout(self):
        self.start_handler(timeout=0)
        self.assertEqual(self.handler.timeout, 0)
        self.assertTrue(self.handler.listening)
        self.assertFalse(self.timeout)

    def test_notify_without_timeout(self):
        self.start_handler(timeout=1)
        self.assertEqual(self.handler.timeout, 1)
        sleep(0.02)
        self.assertFalse(self.timeout)
        self.receive(stop=True)

    def test_notify_with_timeout(self):
        # noinspection PyTypeChecker
        self.start_handler(timeout=0.01)
        sleep(0.02)
        self.assertTrue(self.timeout)


if __name__ == '__main__':
    unittest.main()
