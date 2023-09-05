"""Helpers for memorizing attributes."""

from typing import Any

__all__ = ['AttrDict']


class AttrDict(dict):
    """Simple read-only ordered dictionary for storing attribute names."""

    def __init__(self, *args: Any, **kw: Any) -> None:
        """Initialize the dictionary."""
        self._read_only = False
        super().__init__(*args, **kw)
        self._read_only = True
        error = self._read_only_error
        self.clear = self.update = error  # type: ignore
        self.pop = self.setdefault = self.popitem = error  # type: ignore

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a value."""
        if self._read_only:
            self._read_only_error()
        super().__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        """Delete a value."""
        if self._read_only:
            self._read_only_error()
        super().__delitem__(key)

    @staticmethod
    def _read_only_error(*_args: Any, **_kw: Any) -> Any:
        """Raise error for write operations."""
        raise TypeError('This object is read-only')
