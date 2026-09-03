# Binance USD-M Futures — libro L2 local

Fecha de consulta de la documentación oficial: **2026-09-02**.

Este conector implementa **solo** USD-M Futures (`fstream.binance.com` / `fapi.binance.com`).
No hay checksum de libro en el venue; el sustituto de honestidad es una comparación REST
de los top-N niveles.

## Protocolo oficial (citado)

Fuentes:

- Diff. Book Depth Streams, sección **How to manage a local order book correctly**:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- Espejo histórico (mismo texto, citado por CCXT / barter-data):
  https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
- REST snapshot: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
- Info general WS: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams

Wording implementado (USD-M Futures, **no** Spot):

1. Open a stream to `wss://fstream.binance.com/stream?streams=btcusdt@depth`.
2. Buffer the events you receive from the stream. For same price, latest received update covers the previous one.
3. Get a depth snapshot from `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`.
4. Drop any event where `u` is `< lastUpdateId` in the snapshot.
5. The first processed event should have `U <= lastUpdateId` AND `u >= lastUpdateId`.
6. While listening to the stream, each new event's `pu` should be equal to the previous event's `u`, otherwise initialize the process from step 3.
7. The data in each event is the absolute quantity for a price level.
8. If the quantity is 0, remove the price level.
9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.

En código: `order_flow.ingestion.sync.DepthSynchronizer` + `DepthSequenceValidator`.
El feed **nunca aplica** el evento con gap; cuenta `gaps`/`resyncs` y pide un snapshot nuevo.

## Diferencia vs Spot (honestidad)

Spot documenta:

- stale: `u <= lastUpdateId`
- primer evento: `U <= lastUpdateId+1 AND u >= lastUpdateId+1`
- continuidad: `U == u_prev + 1`

Futures documenta:

- stale: `u < lastUpdateId`
- primer evento: `U <= lastUpdateId AND u >= lastUpdateId`  (**sin** `+1`)
- continuidad: `pu == u` del evento anterior

Un evento con `U = lastUpdateId+1` y `u > lastUpdateId` **pasa Spot y falla Futures**.
Los tests `test_spot_plus_one_rule_is_not_used` y
`test_futures_validator_rejects_spot_plus_one_first_event` bloquean esa confusión.

## Streams y URLs

Sondas en **2026-09-02** (mainnet `fstream.binance.com`):

- Combined `.../stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade`: depth sí, **0 aggTrades**.
- Raw `wss://fstream.binance.com/ws/btcusdt@aggTrade`: handshake OK en una sonda previa, **0 mensajes**.
- Combined `?streams=btcusdt@trade`: prints con `e=trade`, campo `m` idéntico al de aggTrade.
- Más tarde el mismo día, raw `/ws/<sym>@depth@100ms` y `?streams=<sym>@trade` hicieron
  timeout de handshake (20 s); el combined depth+`@trade` sí abrió (~5 s) y trajo ambos.

Por eso el feed **por defecto** combina depth + **`@trade`** (no `@aggTrade`):

```
wss://fstream.binance.com/stream?streams=btcusdt@trade/btcusdt@depth@100ms
```

El orden importa: `depth@100ms/<sym>@trade` en esta máquina (2026-09-02) dejó el
libro casi sin diffs y sí llegó `@trade`. Invertir a **trade primero** entrega
~10 diffs/s y cientos de prints.

`dual_sockets=True` abre depth crudo + trade por separado (útil si combined falla;
en esta máquina a veces es al revés). `scripts/record_l2.py` usa combined
(`dual_sockets=False`) salvo que se cambie el flag.

- Símbolos en minúsculas.
- Depth es obligatorio; trades son opcionales (`include_trades=False` →
  `wss://fstream.binance.com/ws/btcusdt@depth@100ms`).
- Un fallo de parseo de trade **no** tumba el pipeline del libro.
- Combined vs raw: `/stream?streams=a/b` envuelve cada mensaje en
  `{"stream": "...", "data": {...}}`; `/ws/<stream>` envía el payload crudo.
  `unwrap_stream_message` acepta ambos.
- El parser acepta `e=aggTrade` (id `a`) y `e=trade` (id `t`); el flag `m` es el mismo.

Payload `depthUpdate` (campos usados): `E` event time (ms), `T` transaction time (ms),
`s`, `U`, `u`, `pu`, `b`, `a`. La latencia observada es
`ts_recv_ns - ts_event_ns` con `ts_event_ns` tomado de **`E`**.

## Snapshot REST

```
GET https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000
```

`limit` válido: 5, 10, 20, 50, 100, 500, 1000 (default del API: 500; nosotros default **1000**).

Peso documentado (weight):

| limit | weight |
| --- | --- |
| 5, 10, 20, 50 | 2 |
| 100 | 5 |
| 500 | 10 |
| 1000 | 20 |

El budget USD-M es del orden de **2400 weight / minuto / IP**. Un snapshot `limit=1000`
cuesta 20. Nunca se hace busy-loop: HTTP 429 espera `Retry-After` si viene, si no backoff
exponencial (0.5s → 30s) y log `rest_429`.

## Límites WebSocket (documentados)

- 1 conector = 1 WS (esta fase).
- ~**300 conexiones / 5 minutos / IP** (no abrir un WS por símbolo a lo loco).
- ~10 mensajes *entrantes* de control (ping/pong/JSON subscribe) por segundo.
- Conexión válida ~24 h; hay que reconectar con gracia.
- Combined: máximo de streams por conexión (documentado 200–1024 según revisión);
  aquí solo 2 streams.
- Ping/pong: lo maneja `websockets` por defecto; se loguean close codes inesperados.

## Arquitectura del feed

```
WS pump ──► cola interna ──► DepthSynchronizer ──► OrderBook
                                │                      │
                                │ APPLY/DROP/RESYNC    │
                                ▼                      ▼
                         asyncio.Queue ◄── eventos frozen (BookSnapshot/BookDelta/Trade)
```

API:

```python
feed = BinanceFuturesFeed(symbol="BTCUSDT", queue=None)  # crea su cola
await feed.start()
event = await feed.queue.get()
# o: async for event in feed.stream(): ...
feed.book  # no mutar
feed.stats  # gaps, resyncs, reconnects, latency_samples_ns
await feed.stop()
```

- Destino de publicación: **`asyncio.Queue`**. No hay ZeroMQ ni escritura a disco en el feed.
- Tras un corte de socket, `on_disconnect()` tira el buffer y **siempre** se pide snapshot nuevo.
- Reconnect: 0.5s, 1s, 2s, … cap 30s, con jitter.

## Cómo correr la validación en vivo

```bash
RUN_INTEGRATION=1 uv run pytest tests/integration/test_binance_live_l2.py -v -s
# duración: BINANCE_LIVE_SECONDS (default 60)

uv run python scripts/validate_live_l2.py --symbol BTCUSDT --seconds 60
```

El test/script escribe `docs/ingestion/live-validation.md`. No graba Parquet.

Para grabar (capa storage, opcional):

```bash
uv run python scripts/record_l2.py --symbol BTCUSDT --seconds 60 --out data/
uv run python scripts/record_l2.py --help
```
