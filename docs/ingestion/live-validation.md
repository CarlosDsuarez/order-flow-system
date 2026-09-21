# Validación en vivo — Binance USD-M L2

Resultados de un run de honestidad contra **Binance USD-M Futures** en esta máquina.
No es un SLA universal: latencia y reconexiones dependen de la red local.

- **Fecha (UTC):** 2026-09-21 15:02:10Z
- **Símbolo:** `BTCUSDT`
- **Duración pedida:** 60.0 s
- **Duración transcurrida:** 60.0 s
- **Estado:** ejecutado

## Contadores

- Gaps de secuencia (`pu != u` previo): **0**
- Resyncs (nuevo snapshot REST): **0**
- Reconexiones WS: **0**
- HTTP 429: **0**
- Snapshots aplicados: **1**
- Deltas aplicados al libro: **574**
- Deltas vistos en cola (ventana): **574**
- Deltas en vuelo al cerrar la ventana (aplicados/encolados no consumidos): **0**
- Trades (aggTrade): **5760**

## Latencia observada (`ts_recv_ns - ts_event_ns`)

El reloj de evento es el campo oficial `E` (event time, ms) del payload `depthUpdate`.

- n = 6335
- mean = 159.154 ms
- p50 = 99.970 ms
- p99 = 548.697 ms
- min = 94.011 ms
- max = 590.111 ms

## Checksum / comparación REST

Binance USD-M **no publica checksum** del libro. Sustituto: snapshot REST
`GET /fapi/v1/depth` vs top-20 de una copia
congelada del libro local, alineada por `lastUpdateId` (replay de los diffs
bufferizados durante el freeze hasta `u >= rest.lastUpdateId`).

- lastUpdateId local al comparar: `11617414397980`
- lastUpdateId REST: `11617414390788`
- lastUpdateId alineado: `11617414397980`
- delta_ids (`alineado - REST`, debe ser ~0): `7192`
- batches_replayed (≤ 1): `5`
- Stale / no concluyente: **False**
- Niveles comparados: **40**
- Coincidencias: **33**
- Mismatches: **7**
- Máxima discrepancia de qty: **2.036**
- Libro cruzado: **False**
- Niveles (bid, ask): `(3222, 3112)`

### Detalle de mismatches (cap 20)

| side | price | qty_local | qty_rest | abs_diff |
| --- | --- | --- | --- | --- |
| bid | 85910.1 | 5.965 | 3.929 | 2.036 |
| bid | 85910.0 | 0.03 | 0.006 | 0.024 |
| bid | 85909.6 | 0.123 | 0.061 | 0.062 |
| ask | 85910.2 | 0.677 | 2.018 | 1.3409999999999997 |
| ask | 85910.3 | 0.152 | 0.155 | 0.0030000000000000027 |
| ask | 85912.3 | — | 0.001 | 0.001 |
| ask | 85913.0 | 0.001 | 0.234 | 0.233 |

## Conclusión

Veredicto: FAIL — 7/40 mismatches de top-N (tasa 17.50% ≥ warn 10.00%) con comparación congelada y alineada: libro corrupto o regresión.

## Cómo repetir

```bash
RUN_INTEGRATION=1 uv run pytest tests/integration/test_binance_live_l2.py -v -s
# o
uv run python scripts/validate_live_l2.py --symbol BTCUSDT --seconds 60
```
