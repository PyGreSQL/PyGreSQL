"""Helpers for memorizing attributes."""

from typing import Any

__all__ = ['AttrDict']


class AttrDict(dict):
    """Simple read-only ordered dictionary for storing attribute names."""

    def __init__(self, *args: Any, **kw: Any) -> None:
        """Initialize the dictionary."""
        super().__init__(*args, **kw)  # this does not call __setitem__
        error = self._read_only_error
        self.clear = self.update = error  # type: ignore[method-assign]
        self.pop = self.setdefault = self.popitem = error  # type: ignore[method-assign]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a value (not allowed)."""
        self._read_only_error()

    def __delitem__(self, key: str) -> None:
        """Delete a value (not allowed)."""
        self._read_only_error()

    @staticmethod
    def _read_only_error(*_args: Any, **_kw: Any) -> Any:
        """Raise error for write operations."""
        raise TypeError('This object is read-only')
