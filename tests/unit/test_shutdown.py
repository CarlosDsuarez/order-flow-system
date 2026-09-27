"""Graceful-shutdown signal handlers (POSIX; the recorder must flush on SIGTERM)."""

from __future__ import annotations

import asyncio
import os
import signal
import sys

import pytest

from order_flow.utils.shutdown import install_shutdown_handlers

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals only")


async def test_sigterm_runs_callback_instead_of_killing_the_process() -> None:
    default = signal.getsignal(signal.SIGTERM)
    received: list[signal.Signals] = []
    fired = asyncio.Event()

    def on_signal(sig: signal.Signals) -> None:
        received.append(sig)
        fired.set()

    remove = install_shutdown_handlers(on_signal)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(fired.wait(), timeout=1.0)
    finally:
        remove()
    assert received == [signal.SIGTERM]
    assert signal.getsignal(signal.SIGTERM) == default


async def test_second_signal_is_not_intercepted() -> None:
    # One graceful attempt: after the first signal the default action is back, so a
    # second Ctrl-C / SIGTERM still force-kills a shutdown that hangs.
    default = signal.getsignal(signal.SIGTERM)
    fired = asyncio.Event()
    remove = install_shutdown_handlers(lambda _sig: fired.set())
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(fired.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert signal.getsignal(signal.SIGTERM) == default
    finally:
        remove()
