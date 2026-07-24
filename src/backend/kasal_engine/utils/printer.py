"""Printer — colored console output.

Authored module; surface validated against the kasal_engine datamodel.
"""

import sys
from dataclasses import dataclass
from typing import IO, Any

PrinterColor = str

_ANSI: dict[str, str] = {
    "purple": "\033[95m",
    "red": "\033[91m",
    "bold_green": "\033[1m\033[92m",
    "bold_purple": "\033[1m\033[95m",
    "bold_blue": "\033[1m\033[94m",
    "yellow": "\033[93m",
    "bold_yellow": "\033[1m\033[93m",
    "cyan": "\033[96m",
    "bold_cyan": "\033[1m\033[96m",
    "magenta": "\033[35m",
    "bold_magenta": "\033[1m\033[35m",
    "green": "\033[32m",
    "blue": "\033[94m",
    "white": "\033[97m",
    "bold_white": "\033[1m\033[97m",
}
_RESET = "\033[00m"


@dataclass
class ColoredText:
    text: str
    color: PrinterColor | None = None


def _render(text: str, color: PrinterColor | None) -> str:
    code = _ANSI.get(color or "")
    return f"{code}{text}{_RESET}" if code else text


class Printer:
    @staticmethod
    def print(
        content: str | list[ColoredText],
        color: PrinterColor | None = None,
        sep: str | None = " ",
        end: str | None = "\n",
        file: IO[str] | None = None,
        flush: Any = False,
    ) -> None:
        stream = file or sys.stdout
        if isinstance(content, list):
            rendered = (sep or " ").join(
                _render(part.text, part.color or color) for part in content
            )
        else:
            rendered = _render(content, color)
        print(rendered, end=end, file=stream, flush=bool(flush))
