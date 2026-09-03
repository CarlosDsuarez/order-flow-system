# Persistencia Parquet

## Layout

```
<root>/
  snapshots/exchange=binance_futures/symbol=BTCUSDT/date=YYYY-MM-DD/part-NNNNN.parquet
  deltas/exchange=.../symbol=.../date=.../part-NNNNN.parquet
  trades/exchange=.../symbol=.../date=.../part-NNNNN.parquet
```

Los dataclasses siguen usando `EVENT_TYPE` `book_snapshot` / `book_delta` / `trade`.
Las carpetas usan los nombres cortos de arriba. `date` es el día UTC de `ts_event_ns`.

Timestamps: `int64` nanosegundos desde epoch. Niveles L2:
`list<struct<price: f64, qty: f64>>`. Agresor de trade: `aggressor_sign` (+1 buyer,
-1 seller).

Los snapshots guardan el **libro completo en memoria** (típicamente 1000 niveles por
lado, el `limit` REST). No hay recorte top-N.

Cada `flush` escribe un archivo `part-NNNNN.parquet` nuevo (no se reabren row groups).
El recorder vacía por recuento (`buffer_size`, default 2000) y por tiempo
(`--flush-interval`, default 2 s).

El feed **no** escribe disco. `scripts/record_l2.py` se suscribe a `feed.queue`, aplica
un `OrderBook` local, persiste eventos y toma snapshots periódicos del LOB
(`--snapshot-interval`, default 1 s).

## polars vs pyarrow (bench 2026-09-02)

Script: `uv run python scripts/bench_parquet_write.py` (8000 filas, 40 niveles/lado,
zstd, write de un golpe).

| Librería | Tiempo | Filas/s | Tamaño |
| --- | --- | --- | --- |
| polars `DataFrame.write_parquet` | 0.222 s | 36 027 | 82 329 B |
| pyarrow `pq.write_table` | 0.016 s | 506 168 | 112 540 B |

pyarrow gana el dump masivo. Nuestro patrón real es **buffers pequeños cada 1–2 s**
a ~10 deltas/s + trades + un snapshot periódico: miles de filas por segundo, no
cientos de miles. polars a 36k filas/s sigue dejando tres órdenes de margen.

**Writer en producción: polars.** Motivos honestos, no de moda:

1. Ya materializamos eventos a `pl.DataFrame` con schema `Int64` para ns.
2. El lector (`scan_events` / `read_events`) es polars lazy; un writer pyarrow
   duplicaría el schema.
3. No concatenamos row groups: cada flush es un `part-*.parquet` nuevo, API en la
   que ambas librerías son equivalentes.
4. pyarrow queda como dependencia transitiva (polars lo usa por debajo).

Si algún día el cuello es un dump histórico de GB/s, se puede revisar
`pyarrow.dataset` con append de row groups. No hace falta ahora.

## Reconstrucción

`order_flow.storage.reconstruct.reconstruct_book(root, T, exchange=..., symbol=...)`:

1. Cargar el último snapshot con `ts_event_ns <= T` (empate: mayor `last_update_id`).
2. Aplicar deltas con `snapshot_ts < ts_event_ns <= T`, ordenados por
   `(ts_event_ns, final_update_id)`.

Con snapshots periódicos, T cerca de un snapshot aplica pocos deltas. Tests en
`tests/unit/test_reconstruct.py` (datos sintéticos, sin red).

## Consultas: Parquet + DuckDB (siguiente capa)

DuckDB lee Parquet sin servidor. No forma parte del writer ni del install por
defecto. Extra opcional:

```bash
uv sync --extra analytics   # instala duckdb
```

`scripts/capture_report.py` usa **polars** para tasas, tamaños y huecos (>200 ms en
`@depth@100ms`). Si DuckDB está instalado, lanza además un histograma SQL
`COUNT(*) … GROUP BY second`. Si no, lo dice y sigue con polars.

ClickHouse y QuestDB siguen como extras sin usar; no se instalan en el default.
