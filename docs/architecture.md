# Arquitectura

Sistema de investigación de *order flow* en cuatro capas. El código vive bajo el paquete
`order_flow` (layout `src/`). Los identificadores de código están en inglés; esta nota, en español.

## Capas

```
ingestion  →  orderbook + asyncio.Queue  →  storage (Parquet, opcional)
                                          →  metrics (numpy)
                                          →  backtest (nautilus extra `backtest`; hftbacktest extra `hftbacktest`)
```

| Capa | Paquete | Qué hace ahora |
| --- | --- | --- |
| 1. Ingestion | `order_flow.ingestion` | WebSocket público Binance USD-M (`depth@100ms` + `@trade`; el WS `@aggTrade` estuvo silencioso el 2026-09-02), parsers `orjson`, máquina de estados `DepthSynchronizer` con el protocolo **futures** (no Spot), publicación a `asyncio.Queue`. **No escribe disco.** |
| — | `order_flow.orderbook` | Libro L2 (`SortedDict`): snapshot + deltas, mid, spread, microprice, imbalance, `depth_at_level` (1-indexado), `snapshot()`, `is_synced`. Bench: [orderbook/data-structure.md](orderbook/data-structure.md). |
| 2. Storage | `order_flow.storage` | `ParquetWriter` a `snapshots/` `deltas/` `trades/` (hive, ns). Reconstrucción `reconstruct_book`. El feed no toca disco; `scripts/record_l2.py` orquesta. [storage/parquet.md](storage/parquet.md). Captura viva: [storage/live-capture.md](storage/live-capture.md). |
| 3. Metrics | `order_flow.metrics` | OFI, MLOFI, VPIN, CVD como funciones puras. Leer el libro solo si `book.is_synced`. Fórmulas: `docs/math/`. |
| 4. Backtest | `order_flow.backtest` | Tipos `Order`, `Fill`, `Position`, protocolo `Strategy`. Adaptador `nautilus_trader` 1.231.0 (extra `backtest`): Parquet L2 → `OrderBookDeltas` + `TradeTick`. Segundo adaptador `hftbacktest` 2.4.4 (extra `hftbacktest`): Parquet → NumPy `event_dtype` → `ProbQueueModel` + `PowerProbQueueFunc(n=2)`. Misma economía OFI-MM; **no** hay ejecución live. [backtest_limitations.md](backtest_limitations.md), [backtest/hftbacktest_queue.md](backtest/hftbacktest_queue.md). |

Detalle del conector L2: [ingestion/binance-futures-l2.md](ingestion/binance-futures-l2.md).
Libro: [orderbook/data-structure.md](orderbook/data-structure.md).
Parquet: [storage/parquet.md](storage/parquet.md).

## Flujo de un evento L2

1. El pump WS lee frames y las encola (buffer durante el GET REST).
2. Snapshot `GET /fapi/v1/depth`.
3. Se descartan diffs con `u < lastUpdateId`.
4. Primer diff aplicado: `U <= lastUpdateId <= u` (futures, **sin** `+1` de Spot).
5. Siguientes: `pu == u` previo; si no, **resync** (nuevo snapshot, no se aplica el gap).
6. Eventos validados (`BookSnapshot`, `BookDelta`, `Trade`) salen por `feed.queue`.
7. `feed.book` es el libro reconstruido; `is_synced` pasa a `False` en gap/disconnect hasta el siguiente snapshot REST. Los eventos son dataclasses frozen — no mutar el libro.
8. Opcional: `scripts/record_l2.py` aplica la misma cola a un libro local y escribe Parquet (snapshots periódicos + deltas + trades).

## Esquema de eventos

Definido en `order_flow.ingestion.events`:

- `BookSnapshot`: `last_update_id`, `bids`/`asks` como `tuple[PriceLevel, ...]`
- `BookDelta`: `first_update_id`, `final_update_id`, `prev_final_update_id` (`pu`), niveles absolutos (`qty == 0` borra)
- `Trade`: `trade_id`, `price`, `qty`, `aggressor` (`Side.BUY` / `Side.SELL`)

Timestamps: `ts_event_ns` (reloj del exchange; en depth es `E`) y `ts_recv_ns` (local), enteros ns UTC.

## Resync

Cualquier rotura de continuidad (`pu != u` previo, primer evento que no bracket-ea el snapshot, o `SequenceGapError` del libro) **obliga** a un snapshot REST nuevo. Nunca se continúa con un libro posiblemente corrupto. Un disconnect WS tira el buffer: no se confía en mensajes a medio vuelo.

## Qué no está en esta fase

Bybit, OKX, ClickHouse/QuestDB (extras vacíos), ZeroMQ, ticks enteros, capa de
órdenes live. `nautilus_trader` (extra `backtest`) y `hftbacktest` 2.4.4 (extra
`hftbacktest`) sí están como replay. El replay **no** modela latencia de red ni
la cola L3 de Binance; hftbacktest estima cola L2 con ProbQueue. Ver
[backtest_limitations.md](backtest_limitations.md).

## GO / NO-GO (ejecución)

Antes de cualquier capa de órdenes (incluido testnet): [go_no_go_decision.md](go_no_go_decision.md).
Sonda de market data pública, sin órdenes: `scripts/latency_audit.py`
([latency/latency_audit_results.md](latency/latency_audit_results.md)).
**NO-GO** para ejecución en vivo con esta red y el R² lead-1 medido.
