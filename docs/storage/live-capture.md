# Captura L2 en vivo (Parquet)

- Fecha UTC: 2026-09-02 14:12:40Z
- Símbolo: `BTCUSDT`
- Duración pedida: 300.0 s (elapsed 312.355 s)
- Directorio: `data/live-btcusdt-5min`
- Intervalo de snapshots periódicos: 1.0 s

## Volumen

- Símbolo: `BTCUSDT` (`binance_futures`)
- Duración (event time): 298.4 s
- Snapshots: 294
- Deltas: 2926 (9.81 / s)
- Trades: 0 (0.00 / s)
- Tamaño total: 11088191 bytes (snapshots 7706223, deltas 3381968, trades 0)
- Huecos event-time (>200 ms): 0
- Huecos recv-time (>200 ms): 87

- Snapshots REST (cola): 1
- Snapshots periódicos del LOB: 293
- Deltas aplicadas (cola): 2926
- Trades (cola): 0
- Gaps / resyncs / reconnects: 0 / 0 / 0

## Reconstrucción

- Replay al final vs libro en vivo: OK
- Replay a mitad de captura vs snapshot persistido: OK

## Honestidad vs REST (carrera residual)

Binance USD-M no publica checksum de profundidad. Tras el GET `/fapi/v1/depth` pueden llegar diffs `@depth@100ms` antes de comparar. Un puñado de mismatches de cantidad no prueba un libro corrupto; un libro cruzado o una tasa alta sí.

- compared / matches / mismatches: 40 / 36 / 4
- max |Δqty|: 0.4240000000000004
- lastUpdateId local / REST: 11455779513876 / 11455779555400
- libro cruzado: False

## Conclusión

Captura utilizable para reconstruir el libro desde Parquet.

- **5 minutos** de wall-clock (pedido 300 s, elapsed 312 s, event-time 298.4 s). No se
  corrió una toma de 10 min; `scripts/capture_report.py` funciona con cualquier
  duración en el directorio.
- Histograma polars de deltas/s: 300 segundos, min 2, mediana 10, max 10, media 9.75
  (coherente con `@depth@100ms`).
- Tamaño: **10.6 MiB** (snapshots 7.35 MiB, deltas 3.23 MiB, trades 0).
- Reconstrucción al final: BBO `77213.3 / 77213.4` idéntico al libro en vivo
  (`lastUpdateId` 11455779513876). A mitad de captura: spread 0.1, no cruzado.
- Honestidad REST top-20: 36/40 match, 4 mismatches, max |Δqty| 0.42. El
  `lastUpdateId` REST (11455779555400) va por delante del local: carrera residual,
  sin checksum de venue.
- Huecos **event-time** 0 (el stream del exchange es continuo). Huecos **recv-time**
  87 (>200 ms): coinciden con flushes Parquet de snapshots ~1000 niveles; el loop
  asyncio se bloquea al escribir, no se pierden diffs (event-time limpio).
- **0 aggTrade** en 5 min, igual que la fase 2. Probe posterior (2026-09-02): el
  socket dedicado `wss://fstream.binance.com/ws/btcusdt@aggTrade` conectó (tras
  timeouts de handshake) y recibió **0 mensajes en 5 s**. Combined invertido
  (`aggTrade` primero, luego `depth@100ms`) entregó **50 depths y 0 trades**.
  El schema y la ruta `trades/` están listos para CVD; el pipeline de trades no
  se ejercitó en vivo. Sospecha: filtrado regional / producto, no un bug de
  parser. Handshake a `fstream` es intermitente (varios `TimeoutError` de 8 s).

Comando:

```bash
uv run python scripts/record_l2.py --symbol BTCUSDT --seconds 300 \
  --out data/live-btcusdt-5min --snapshot-interval 1 \
  --report docs/storage/live-capture.md
uv run python scripts/capture_report.py data/live-btcusdt-5min
```

