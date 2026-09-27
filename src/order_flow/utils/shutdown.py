"""Graceful shutdown on SIGINT / SIGTERM for long-running asyncio captures.

``asyncio.run`` installs no SIGTERM handler, so ``kill``, launchd or ``docker stop`` end
the process without running ``finally`` blocks: buffered Parquet rows and
``capture_meta.json`` are lost. The handlers below turn the first signal into a callback
(set a stop event, let the capture loop exit and flush). They are one-shot: after the
first signal the default action is restored, so a second signal still force-kills a
shutdown that hangs.
"""

from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

DEFAULT_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)


def install_shutdown_handlers(
    on_signal: Callable[[signal.Signals], None],
    signals: Iterable[signal.Signals] = DEFAULT_SIGNALS,
) -> Callable[[], None]:
    """Route ``signals`` to ``on_signal`` on the running loop; return an uninstaller.

    Platforms without :meth:`asyncio.AbstractEventLoop.add_signal_handler` (Windows) are a
    silent no-op: default behaviour is kept.
    """
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def remove() -> None:
        for sig in installed:
            loop.remove_signal_handler(sig)
        installed.clear()

    def handle(sig: signal.Signals) -> None:
        remove()
        on_signal(sig)

    for sig in signals:
        try:
            loop.add_signal_handler(sig, handle, sig)
        except NotImplementedError:
            continue
        installed.append(sig)
    return remove
