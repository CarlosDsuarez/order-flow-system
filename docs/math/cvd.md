# CVD — Cumulative Volume Delta

Implementación: `order_flow.metrics.cvd` (núcleo numpy),
`order_flow.metrics.stream.CvdAccumulator` (streaming) y
`order_flow.metrics.batch` (trades Parquet → mismos arrays).

CVD es una métrica de **práctica de mercado**, no un estimador de un paper
de microestructura con teorema. Lo que sí tiene fundamento académico es la
**clasificación de la dirección del trade**.

## Fuente de la clasificación (no de la suma)

Lee, C. M. C. & Ready, M. J. (1991). *Inferring Trade Direction from
Intraday Data*. Journal of Finance, 46(2), 733–746.

Lee y Ready combinan el *quote test* (trade en o por encima del ask ⇒ compra
iniciada; en o por debajo del bid ⇒ venta iniciada) con el *tick test* para
prints dentro del spread. En equity TAQ esa inferencia es necesaria porque
el tape no publica el agresor.

En futuros cripto **no se infiere**: Binance publica el lado del maker. CVD
solo suma volumen con el signo de ese agresor.

## Fórmula

Sea \(v_i\) el volumen del print \(i\) y \(s_i \in \{+1,-1\}\) el signo del
agresor (\(s_i = +1\) si el taker compró, \(-1\) si vendió):

\[
\delta_i = s_i v_i,
\qquad
\mathrm{CVD}_t = \sum_{i \le t} \delta_i
\]

`compute_trade_delta` es \(\delta_i\); `compute_cvd` es la suma acumulada.
`resample_cvd` agrega \(\delta_i\) a barras de tiempo fijas (origen Unix por
defecto), incluyendo barras vacías con delta 0 para que el acumulado sea una
función escalón.

## Mapping Binance USD-M: campo `m` (contra-intuitivo)

Documentación oficial (USD-M Futures, Aggregate Trade Streams), recuperada
el **2026-09-02**:

- https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams

El payload de `<symbol>@aggTrade` incluye el booleano `m`. La frase oficial
en el comentario del campo (idéntica en el spec público de WebSocket streams
de Binance; [web-socket-streams.md](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md),
sección *Aggregate Trade Streams*, consultada 2026-09-02) es:

> `"m": true,              // Is the buyer the market maker?`

**`m = true` ⇒ el comprador es el maker ⇒ el agresor (taker) es el
vendedor ⇒ \(\delta_i = -v_i\).**

**`m = false` ⇒ el comprador no es el maker ⇒ el agresor es el comprador
⇒ \(\delta_i = +v_i\).**

En código: `parse_agg_trade` / `parse_trade` hacen
`aggressor = Side.SELL if bool(msg["m"]) else Side.BUY`.
`Side.SELL.sign == -1`. No invertir este flag: es la fuente #1 de CVD con
signo al revés.

### Hallazgo WS 2026-09-02: `@aggTrade` silencioso, `@trade` sí llega

REST `GET /fapi/v1/aggTrades` responde con el campo `m`. El WebSocket
documentado `<symbol>@aggTrade` en `wss://fstream.binance.com/ws/...` **no
entregó ningún mensaje** en sondas de 15 s+ (handshake OK, 0 frames). El
stream `<symbol>@trade` sí (cientos de prints en ~8 s), con el **mismo**
booleano `m` más campos extra (`X`, `st`). El parser acepta ambos
(`e == "aggTrade"` usa id `a`; `e == "trade"` usa id `t`).

El recorder abre **dos sockets** si `dual_sockets=True` (ambos por
`/stream?streams=`, no el raw `/ws/` que en esta IP hizo timeout):

1. Depth: `wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms`
2. Trades: `wss://fstream.binance.com/stream?streams=btcusdt@trade`

Por defecto el recorder usa **un** combined con **trade primero**:
`?streams=btcusdt@trade/btcusdt@depth@100ms` (el orden inverso dejó el
libro casi sin diffs el 2026-09-02).

Combined `?streams=depth@100ms/<sym>@trade` sigue disponible en
`BinanceFuturesFeed` (una conexión; `dual_sockets=False`). Combined envuelve
`{"stream": "...", "data": {...}}`; `unwrap_stream_message` acepta ambos
envelopes.

Documentación USD-M Trade Streams:
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trade-Streams
(consultado 2026-09-02).

## Notas de implementación

- Solo trades con `qty > 0`. Signos distintos de \(\pm 1\) se rechazan.
- El acumulador streaming y la función batch aplican la **misma**
  `compute_trade_delta` / `cumsum`.
- Si una captura no tiene filas en `trades/`, CVD no se puede validar en
  datos reales (OFI no depende de trades). Ver `docs/math/ofi_validation.md`.

## Parámetros

| Parámetro | Valor |
| --- | --- |
| Signo | agresor publicado (`m`), no Lee-Ready |
| Unidad de \(v_i\) | cantidad del contrato (BTC en BTCUSDT USD-M) |
| Barras | opcional; 1 s / 5 s / 10 s alineadas al mismo origen que OFI |

## Citación

Lee, Charles M. C. y Mark J. Ready. 1991. “Inferring Trade Direction from
Intraday Data.” *Journal of Finance* 46 (2): 733–746.

Binance. “Aggregate Trade Streams.” USD-M Futures WebSocket Market Streams.
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
(consultado 2026-09-02).

Binance. “Trade Streams.” USD-M Futures WebSocket Market Streams.
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trade-Streams
(consultado 2026-09-02).
