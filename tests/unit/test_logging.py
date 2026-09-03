"""structlog configuration renders JSON or console output and validates levels."""

from __future__ import annotations

import json

import pytest

from order_flow.utils.logging import configure_logging, get_logger


def test_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("DEBUG", json_output=True)
    get_logger("test.module").info("hello", answer=42)
    line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "hello"
    assert record["answer"] == 42
    assert record["level"] == "info"
    assert record["logger"] == "test.module"
    assert "timestamp" in record


def test_level_filtering(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("warning", json_output=True)
    logger = get_logger()
    logger.info("dropped")
    logger.warning("kept")
    out = capsys.readouterr().out
    assert "dropped" not in out
    assert "kept" in out


def test_console_output(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=False)
    get_logger("console").info("readable", key="value")
    out = capsys.readouterr().out
    assert "readable" in out
    assert "key=" in out


def test_auto_mode_picks_json_when_not_a_tty(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")  # pytest capture is not a TTY
    get_logger().info("auto")
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["event"] == "auto"


def test_unknown_level_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown log level"):
        configure_logging("LOUD")
