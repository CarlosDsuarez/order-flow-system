"""structlog configuration: JSON lines for machines, coloured console for humans."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger, Processor


def configure_logging(level: str = "INFO", *, json_output: bool | None = None) -> None:
    """Configure ``structlog`` and the stdlib root logger.

    Args:
        level: Level name such as ``"DEBUG"`` or ``"INFO"`` (case-insensitive).
        json_output: ``True`` emits one JSON object per line (production),
            ``False`` renders a human-friendly console format (coloured only when stdout
            is a terminal). ``None`` (default) picks JSON when stdout is not interactive.

    Raises:
        ValueError: If ``level`` is not a known logging level name.
    """
    level_num = logging.getLevelNamesMapping().get(level.upper())
    if level_num is None:
        msg = f"Unknown log level: {level!r}"
        raise ValueError(msg)
    is_tty = sys.stdout.isatty()
    if json_output is None:
        json_output = not is_tty

    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=is_tty)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level_num),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    logging.basicConfig(level=level_num, format="%(message)s", stream=sys.stdout, force=True)


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Return a bound structlog logger, tagged with ``logger=name`` when a name is given."""
    logger: FilteringBoundLogger = structlog.get_logger()
    if name is not None:
        logger = logger.bind(logger=name)
    return logger
