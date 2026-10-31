"""
fmt.py — Lightweight terminal formatting utilities (stdlib-only).

Provides:
  • print_rule(title)          – a horizontal divider with centred title
  • print_table(headers, rows) – a Unicode-box ASCII table
  • print_panel(text, title)   – a bordered panel
  • cprint(text, color)        – ANSI-coloured print
"""

from __future__ import annotations

import shutil
import sys

# ─── ANSI colours ─────────────────────────────────────────────────────────────
_COLORS = {
    "reset":   "\033[0m",
    "bold":    "\033[1m",
    "cyan":    "\033[36m",
    "green":   "\033[32m",
    "red":     "\033[31m",
    "yellow":  "\033[33m",
    "magenta": "\033[35m",
    "dim":     "\033[2m",
    "blue":    "\033[34m",
}

_USE_COLOR = sys.stdout.isatty()


def _c(text: str, *colors: str) -> str:
    if not _USE_COLOR:
        return text
    codes = "".join(_COLORS.get(c, "") for c in colors)
    return f"{codes}{text}{_COLORS['reset']}"


def cprint(text: str, *colors: str) -> None:
    print(_c(text, *colors))


# ─── Terminal width ────────────────────────────────────────────────────────────

def _term_width() -> int:
    return shutil.get_terminal_size((100, 24)).columns


# ─── Rule ─────────────────────────────────────────────────────────────────────

def print_rule(title: str = "", char: str = "─") -> None:
    w = _term_width()
    if title:
        pad   = max(0, w - len(title) - 2)
        left  = pad // 2
        right = pad - left
        line  = char * left + " " + title + " " + char * right
    else:
        line = char * w
    print(_c(line[:w], "cyan", "bold"))


# ─── Table ────────────────────────────────────────────────────────────────────

def print_table(
    title: str,
    headers: list[str],
    rows: list[list[str]],
    col_align: list[str] | None = None,   # "l", "r", "c" per column
) -> None:
    """
    Print a Unicode-box table.

    Parameters
    ----------
    title : str
        Caption printed above the table.
    headers : list[str]
    rows : list[list[str]]
        Each inner list must have the same length as headers.
    col_align : list[str], optional
        Alignment per column: "l"=left, "r"=right, "c"=centre.
    """
    n_cols = len(headers)
    if col_align is None:
        col_align = ["l"] * n_cols

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def _cell(text: str, width: int, align: str) -> str:
        text = str(text)
        if align == "r":
            return text.rjust(width)
        elif align == "c":
            return text.center(width)
        else:
            return text.ljust(width)

    # Build border lines
    top  = "┌" + "┬".join("─" * (w + 2) for w in widths) + "┐"
    mid  = "├" + "┼".join("─" * (w + 2) for w in widths) + "┤"
    bot  = "└" + "┴".join("─" * (w + 2) for w in widths) + "┘"
    row_sep = "│"

    def _row_line(cells: list[str], bold: bool = False) -> str:
        parts = [
            " " + _cell(c, widths[i], col_align[i]) + " "
            for i, c in enumerate(cells)
        ]
        line = row_sep + row_sep.join(parts) + row_sep
        return _c(line, "bold") if bold else line

    if title:
        print(_c(f"\n  {title}", "bold", "cyan"))
    print(top)
    print(_row_line(headers, bold=True))
    print(mid)
    for row in rows:
        print(_row_line(row))
    print(bot)
    print()


# ─── Panel ────────────────────────────────────────────────────────────────────

def print_panel(text: str, title: str = "") -> None:
    w = _term_width()
    inner_w = w - 4

    lines = text.split("\n")
    top = "╔" + ("═" * (w - 2)) + "╗"
    bot = "╚" + ("═" * (w - 2)) + "╝"

    print(_c(top, "blue", "bold"))
    if title:
        print(_c("║ " + _c(title.center(w - 4), "bold", "magenta") + " ║", "blue", "bold"))
        print(_c("╠" + ("═" * (w - 2)) + "╣", "blue", "bold"))
    for line in lines:
        padded = line[:inner_w].ljust(inner_w)
        print(_c("║ ", "blue", "bold") + padded + _c(" ║", "blue", "bold"))
    print(_c(bot, "blue", "bold"))
    print()
