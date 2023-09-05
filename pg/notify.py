"""Handling of notifications."""

from __future__ import annotations

import select
from typing import TYPE_CHECKING, Callable

from .core import Query
from .error import db_error

if TYPE_CHECKING:
    from .db import DB

__all__ = ['NotificationHandler']

# The notification handler

class NotificationHandler:
    """A PostgreSQL client-side asynchronous notification handler."""

    def __init__(self, db: DB, event: str, callback: Callable,
                 arg_dict: dict | None = None,
                 timeout: int | float | None = None,
                 stop_event: str | None = None):
        """Initialize the notification handler.

        You must pass a PyGreSQL database connection, the name of an
        event (notification channel) to listen for and a callback function.

        You can also specify a dictionary arg_dict that will be passed as
        the single argument to the callback function, and a timeout value
        in seconds (a floating point number denotes fractions of seconds).
        If it is absent or None, the callers will never time out.  If the
        timeout is reached, the callback function will be called with a
        single argument that is None.  If you set the timeout to zero,
        the handler will poll notifications synchronously and return.

        You can specify the name of the event that will be used to signal
        the handler to stop listening as stop_event. By default, it will
        be the event name prefixed with 'stop_'.
        """
        self.db: DB | None = db
        self.event = event
        self.stop_event = stop_event or f'stop_{event}'
        self.listening = False
        self.callback = callback
        if arg_dict is None:
            arg_dict = {}
        self.arg_dict = arg_dict
        self.timeout = timeout

    def __del__(self) -> None:
        """Delete the notification handler."""
        self.unlisten()

    def close(self) -> None:
        """Stop listening and close the connection."""
        if self.db:
            self.unlisten()
            self.db.close()
            self.db = None

    def listen(self) -> None:
        """Start listening for the event and the stop event."""
        db = self.db
        if db and not self.listening:
            db.query(f'listen "{self.event}"')
            db.query(f'listen "{self.stop_event}"')
            self.listening = True

    def unlisten(self) -> None:
        """Stop listening for the event and the stop event."""
        db = self.db
        if db and self.listening:
            db.query(f'unlisten "{self.event}"')
            db.query(f'unlisten "{self.stop_event}"')
            self.listening = False

    def notify(self, db: DB | None = None, stop: bool = False,
               payload: str | None = None) -> Query | None:
        """Generate a notification.

        Optionally, you can pass a payload with the notification.

        If you set the stop flag, a stop notification will be sent that
        will cause the handler to stop listening.

        Note: If the notification handler is running in another thread, you
        must pass a different database connection since PyGreSQL database
        connections are not thread-safe.
        """
        if not self.listening:
            return None
        if not db:
            db = self.db
            if not db:
                return None
        event = self.stop_event if stop else self.event
        cmd = f'notify "{event}"'
        if payload:
            cmd += f", '{payload}'"
        return db.query(cmd)

    def __call__(self) -> None:
        """Invoke the notification handler.

        The handler is a loop that listens for notifications on the event
        and stop event channels.  When either of these notifications are
        received, its associated 'pid', 'event' and 'extra' (the payload
        passed with the notification) are inserted into its arg_dict
        dictionary and the callback is invoked with this dictionary as
        a single argument.  When the handler receives a stop event, it
        stops listening to both events and return.

        In the special case that the timeout of the handler has been set
        to zero, the handler will poll all events synchronously and return.
        If will keep listening until it receives a stop event.

        Note: If you run this loop in another thread, don't use the same
        database connection for database operations in the main thread.
        """
        if not self.db:
            return
        self.listen()
        poll = self.timeout == 0
        rlist = [] if poll else [self.db.fileno()]
        while self.db and self.listening:
            # noinspection PyUnboundLocalVariable
            if poll or select.select(rlist, [], [], self.timeout)[0]:
                while self.db and self.listening:
                    notice = self.db.getnotify()
                    if not notice:  # no more messages
                        break
                    event, pid, extra = notice
                    if event not in (self.event, self.stop_event):
                        self.unlisten()
                        raise db_error(
                            f'Listening for "{self.event}"'
                            f' and "{self.stop_event}",'
                            f' but notified of "{event}"')
                    if event == self.stop_event:
                        self.unlisten()
                    self.arg_dict.update(pid=pid, event=event, extra=extra)
                    self.callback(self.arg_dict)
                if poll:
                    break
            else:   # we timed out
                self.unlisten()
                self.callback(None)