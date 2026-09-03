"""ClickHouse / QuestDB skeletons fail loudly and point to the right extra."""

from __future__ import annotations

from importlib.machinery import ModuleSpec

import pytest

from order_flow.storage import clickhouse, questdb


def test_clickhouse_requires_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(clickhouse, "find_spec", lambda _name: None)
    with pytest.raises(ImportError, match="uv sync --extra clickhouse"):
        clickhouse.ClickHouseSink()


def test_clickhouse_not_implemented_when_driver_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(clickhouse, "find_spec", lambda name: ModuleSpec(name, None))
    with pytest.raises(NotImplementedError, match="phase 2"):
        clickhouse.ClickHouseSink(host="db", port=9000)


def test_questdb_requires_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(questdb, "find_spec", lambda _name: None)
    with pytest.raises(ImportError, match="uv sync --extra questdb"):
        questdb.QuestDBSink()


def test_questdb_not_implemented_when_driver_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(questdb, "find_spec", lambda name: ModuleSpec(name, None))
    with pytest.raises(NotImplementedError, match="phase 2"):
        questdb.QuestDBSink()


def test_sink_methods_are_not_implemented() -> None:
    sink = clickhouse.ClickHouseSink.__new__(clickhouse.ClickHouseSink)
    with pytest.raises(NotImplementedError):
        sink.write([])
    with pytest.raises(NotImplementedError):
        sink.flush()
    with pytest.raises(NotImplementedError):
        sink.close()
