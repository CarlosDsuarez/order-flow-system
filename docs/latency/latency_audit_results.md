# Auditoría de latencia — Binance USD-M (esta máquina)

Sonda de market data **pública**. No envía órdenes. Reloj de evento: campo `E` (ms).

- **Fecha (UTC):** 2026-09-02 19:40:59Z
- **Host:** `Carloair.local`
- **Contexto:** red de investigación doméstica, no colocación
- **Símbolo:** `BTCUSDT`
- **Streams:** `btcusdt@depth@100ms, btcusdt@trade`
- **n depth pedido:** 10000
- **n depth recibido:** 10000
- **n trade recibido:** 36092
- **Duración:** 1221.7 s
- **Reconnects WS:** 3
- **Gaps de secuencia (`pu != u` previo):** 0
- **Errores de decode:** 0

## Offset de reloj (`offset = local - server`)

Muestras `GET https://fapi.binance.com/fapi/v1/time` (`serverTime`) al inicio, a mitad y al final. `offset_ns = local_after_ns - serverTime_ns`. Offset negativo = reloj local detrás de Binance. El mid-point NTP se reporta como control; el ajuste de latencia usa la media del offset `local - server`.

- n muestras: 15
- mean offset: -196.678 ms
- min / p50 / p90 / p99 / p99.9 / max offset: -341.132 / -241.940 / -88.195 / 348.581 / 404.146 / 410.320 ms
- mean midpoint offset: -394.344 ms
- RTT medio HTTP a `/fapi/v1/time`: 395.332 ms; máx 932.400 ms. La incertidumbre residual del offset es del orden de RTT/2 por asimetría de ruta, más el error de NTP del OS. Exchange-local en wall clock **incluye** ese offset.

**Latencia cruda negativa:** el reloj local va **atrás** del `E` de Binance (offset `local - server` negativo). **No es ventaja frente a colo** y no es procesamiento más rápido que la luz. Se reporta cruda y ajustada por offset; el ajuste estima red+parse con incertidumbre residual (NTP, RTT HTTP, asimetría).

## Distribución de latencia (ms)

Cruda: `recv_wall - E`. Ajustada: `recv_wall - E - offset`. `E` es event time oficial (ms). `T` (transaction time) se registra pero no define la latencia. `time.time_ns()` vs UTC del exchange; `time.perf_counter_ns()` solo para inter-llegada local.

| Serie | n | min | p50 | p90 | p99 | p99.9 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| depth cruda | 10000 | -358.101 | -305.421 | -112.512 | 585.864 | 3750.543 | 4646.970 | -234.285 |
| depth ajustada | 10000 | -161.423 | -108.742 | 84.166 | 782.543 | 3947.221 | 4843.648 | -37.607 |
| trade cruda | 36092 | -357.712 | -280.252 | 34.982 | 3387.809 | 4430.166 | 4542.026 | -122.663 |
| trade ajustada | 36092 | -161.034 | -83.574 | 231.660 | 3584.487 | 4626.845 | 4738.704 | 74.016 |

## Inter-llegada y huecos

- Umbral de hueco: 250 ms
- Huecos event-time `E` (depth): 0
- Huecos local monotonic (depth): 589
- Inter-llegada `E` depth p50/p99: 102.000 / 102.000 ms
- Inter-llegada local depth p50/p99: 99.106 / 420.706 ms

## Comparación institucional (órdenes de magnitud)

| Fuente | Cifra | Notas | URL | Recuperado |
| --- | --- | --- | --- | --- |
| Databento (marketing cuantitativo) | p90 42 µs (cross-connect) / 590 µs (internet) hasta la aplicación; mediana 6.1 µs handoff→envío en el gateway | Cifras de producto, no un SLA de Binance. Orden de magnitud colo vs internet. | https://databento.com/live | 2026-09-02 |
| Databento dedicated connectivity (marketing) | p90 42.4 µs cross-connect 10G/25G; internet 0.5+ ms; interconnect cloud 1.7+ ms | Tabla de arquitectura propia. Marketing, pero con números explícitos. | https://databento.com/docs/architecture/dedicated-connectivity-guide | 2026-09-02 |
| Nanoconda — CME MDP3 vs iLink (empírico colocated) | MD latency mediana 265.7 µs; MSGW 203.1 µs (exchange sending time - transaction time) | Medición con timestamps CME, no reloj local. Colo Aurora, no retail. | https://nanoconda.com/blog/cme-trade-summary-vs-private-fills/ | 2026-09-02 |
| Rithmic R|API suite (marketing de vendor) | Diamond API tick-to-trade típico <250 µs (colo); R/API+ / Protocol <1 ms | Especificación comercial. No es Binance. CQG no publica µs comparables. | https://www.rithmic.com/products/api-suite | 2026-09-02 |
| CQG Client APIs (marketing) | «the CQG API introduces only one millisecond for data round-trip» | Folleto, no hop de matching engine. Sin spec pública en µs. | https://www.cqg.com/products/cqg-apis/client-apis | 2026-09-02 |
| Binance USD-M Diff. Book Depth (oficial) | Update speed 250 ms / 500 ms / 100 ms (`@depth@100ms`) | El libro que alimenta OFI ya está discretizado a 100 ms. Eso puede dominar cualquier last-mile de unos pocos ms. | https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams | 2026-09-02 |
| Binance Developer Community (anecdótico, no SLA) | WS API USD-M RTT ~6 ms vs Spot ~1.6 ms (un usuario, 2024-07) | Hilo de foro. Infra de proximidad, no hogar. No generalizar. | https://dev.binance.vision/t/usd-m-futures-high-websocket-api-latency/21511 | 2026-09-02 |
| Jane Street engineering blog (orden de magnitud) | Sistemas de trading que responden en «far less than» 250 µs (el intervalo de un profiler muestral) | No es un SLA. Sitúa el tick-to-trade colo en cientos de µs o menos. | https://blog.janestreet.com/magic-trace/ | 2026-09-02 |

Esta máquina es **red de investigación / hogar, no colo**. Comparar p99 / p99.9 ajustados (no la media) contra decenas-cientos de **µs** en colo y contra el throttle oficial de **100 ms** del depth. Un p99 de decenas de ms es ~100-1000x un hop colocated típico de market data.

## Cómo repetir

```bash
uv run python scripts/latency_audit.py --symbol BTCUSDT --n-events 10000
```
