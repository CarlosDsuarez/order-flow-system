# Validación en vivo — Binance USD-M L2

Resultados de un run de honestidad contra **Binance USD-M Futures** en esta máquina.
No es un SLA universal: latencia y reconexiones dependen de la red local.

- **Fecha (UTC):** 2026-09-02 13:29:48Z
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
- Deltas aplicados: **581**
- Trades (aggTrade): **0**

## Latencia observada (`ts_recv_ns - ts_event_ns`)

El reloj de evento es el campo oficial `E` (event time, ms) del payload `depthUpdate`.

- n = 582
- mean = -34.065 ms
- p50 = -34.453 ms
- p99 = -27.205 ms
- min = -35.992 ms
- max = -2.708 ms

## Checksum / comparación REST

Binance USD-M **no publica checksum** del libro. Sustituto: snapshot REST
`GET /fapi/v1/depth` vs top-20 local
(por precio). Carrera residual: 1-N diffs de 100 ms pueden caer entre el catch-up
de `lastUpdateId` y la copia de niveles.

- lastUpdateId local: `11455319759723`
- lastUpdateId REST: `11455319758344`
- Niveles comparados: **40**
- Coincidencias: **39**
- Mismatches: **1**
- Máxima discrepancia de qty: **0.016000000000000014**
- Libro cruzado: **False**
- Niveles (bid, ask): `(2678, 2931)`

## Conclusión

El pipeline aplicó 581 deltas sin gaps de secuencia y la comparación REST top-N quedó en 1/40 mismatches (carrera residual esperada). **Es lo bastante honesto para construir la siguiente capa encima**, con la salvedad de que esto es la red de esta máquina, no un SLA.

## Notas de esta máquina

- **Latencia negativa (~ −34 ms):** `ts_recv_ns - E` salió negativo. Eso es **skew de reloj local vs Binance** (el portátil va ~34 ms atrasado respecto al `E` del venue), no un delay de procesamiento. El p99 es menos negativo que la media; el spread es de unos 30 ms.
- **0 aggTrade en 60 s:** el combined stream está suscrito (`btcusdt@depth@100ms/btcusdt@aggTrade`). Un probe aparte a `wss://fstream.binance.com/ws/btcusdt@aggTrade` abrió el socket y no recibió prints en 5 s. El libro L2 (objetivo de esta fase) sí fluyó. Los trades son opcionales; merece un follow-up si Carlos quiere CVD/VPIN en vivo.
- **lastUpdateId local > REST:** el catch-up deja el libro unas actualizaciones por delante del snapshot de honestidad (carrera residual de `@depth@100ms`). 1 mismatch / 40 niveles, qty 0.016, es coherente con eso.
- El libro creció a ~2700 niveles/lado porque los diffs añaden precios que no estaban en el snapshot de 1000 (comportamiento documentado por Binance).

## Cómo repetir

```bash
RUN_INTEGRATION=1 uv run pytest tests/integration/test_binance_live_l2.py -v -s
# o
uv run python scripts/validate_live_l2.py --symbol BTCUSDT --seconds 60
```
