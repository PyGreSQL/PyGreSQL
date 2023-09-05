"""Timezone helpers."""

from __future__ import annotations

__all__ = ['timezone_as_offset']

# time zones used in Postgres timestamptz output
_timezone_offsets: dict[str, str] = {
    'CET': '+0100', 'EET': '+0200', 'EST': '-0500',
    'GMT': '+0000', 'HST': '-1000', 'MET': '+0100', 'MST': '-0700',
    'UCT': '+0000', 'UTC': '+0000', 'WET': '+0000'
}


def timezone_as_offset(tz: str) -> str:
    """Convert timezone abbreviation to offset."""
    if tz.startswith(('+', '-')):
        if len(tz) < 5:
            return tz + '00'
        return tz.replace(':', '')
    return _timezone_offsets.get(tz, '+0000')