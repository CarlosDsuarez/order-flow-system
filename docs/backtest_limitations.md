# Limitaciones del backtest (léelo antes que el PnL)

Este documento es deliberadamente **brusco**. El backtest de la fase 6
(`nautilus_trader` 1.231.0 sobre Parquet L2) es un **test de tubería**:
datos → OFI L1 → decisión de sesgo → orden simulada. **No** es una prueba de
que exista un edge de market making, ni de que OFI prediga el mid.

Resultados numéricos: [backtest/ofi_mm_results.md](backtest/ofi_mm_results.md).
Validación empírica de OFI en la misma captura:
[math/ofi_validation.md](math/ofi_validation.md).

## Lo que el motor no modela

- **Latencia de red.** No hay `latency_model`. El reloj del backtest es el
  `ts_event` del exchange. En la captura real, recv−event p99 fue ~180 ms. Un
  maker que “está” en el libro histórico a T no habría llegado a T en vivo.
- **Posición real en cola / prioridad.** El libro es L2 (MBP). No hay
  `order_id` verdadero por orden. nautilus 1.231, con `queue_position=True`,
  **estima** FIFO a partir del tamaño *mostrado* en el nivel en el momento de
  aceptar la orden: trades del lado correcto reducen “lo que hay delante”; un
  DELETE del nivel limpia la cola; un UPDATE recorta la cola al nuevo tamaño.
  Eso **no** es la cola de Binance. Órdenes ocultas, icebergs, self-trade
  prevention, cola de lotes, y prioridad temporal entre *nuestros* peers no
  existen en el tape.
  La cola queda **parcialmente** modelada también en `hftbacktest` 2.4.4
  (`ProbQueueModel` + `PowerProbQueueFunc(n=2)`; ver
  [backtest/hftbacktest_queue.md](backtest/hftbacktest_queue.md) y
  [backtest/queue_position_comparison.md](backtest/queue_position_comparison.md)):
  las bajas de size se parten delante/detrás con \(f(x)=x^2\). Sigue sin haber
  órdenes de competidores reales ni MBO. El Δ de fill rate vs nautilus en
  15 min fue **+1.3 pp** — L2 a 100 ms sigue dominando el error de cola.
- **Impacto en el libro histórico.** Los fills simulados **no editan** el
  order book grabado. El matching usa el libro como escenario inmutable y, en
  trades, mueve referencias transitorias de precio. En vivo, 0.001 BTC no
  mueve BTCUSDT; a tamaño real sí. Aquí el tamaño es de juguete a propósito.
- **Funding, mark price, liquidaciones, disconnects.** Sin tasa de funding,
  sin mark price, sin liquidación (`liquidation_enabled` no está en
  `add_venue` 1.231; no se activa). La captura tuvo reconnects WS; el replay
  es un tape continuo ya sincronizado.
- **Competencia.** Nadie más cotiza. No hay queue jumpers, no hay last-look,
  no hay post-only races contra otros makers.

## Cómo rellena nautilus 1.231 (lo que *sí* hace)

Documentado en
[Trade-Based Execution](https://nautilustrader.io/docs/latest/concepts/backtesting/trade-execution/)
y en `BacktestVenueConfig` de 1.231.0. Este proyecto usa:

| Flag | Valor | Efecto |
| --- | --- | --- |
| `book_type` | `L2_MBP` | Solo `OrderBookDelta(s)` actualizan el libro. Quotes/bars se ignoran. |
| `trade_execution` | `True` | Cada `TradeTick` dispara matching. |
| `queue_position` | `True` | Cola estimada (tamaño mostrado). |
| `liquidity_consumption` | `True` | Los fills de un tick no pueden superar el size no consumido del trade. |
| `latency_model` | `None` | Sin delay de orden/feed. |
| `MakerTakerFeeModel` | fees del `CryptoPerpetual` | 0.02% maker / 0.04% taker en las corridas por defecto. |

Reglas de fill por trade (oficiales):

1. Un trade `SELL` (agresor vendedor, nuestro `Side.SELL` / `m=true` en
   Binance) puede llenar **bids** pasivos. Un trade `BUY` puede llenar
   **asks** pasivos.
2. Si el libro tiene liquidez cruzada, se camina el libro. Si el precio del
   trade no está en el libro, puede haber un fill “trade-driven” al precio
   *límite de la orden*, acotado a `min(leaves_qty, trade.size)`.
3. El libro histórico **no cambia** por nuestros fills.
4. `NO_AGGRESSOR` no aparece en esta captura (siempre hay `m`); si apareciera,
   nautilus reduciría cola en **ambos** lados — sesgo **optimista**.

L2 no puede saber si estábamos los primeros de la cola. `queue_position` es
una cota a partir del size visible; es **mejor que rellenar al toque**, y
**peor que L3**.

La estrategia maker usa `post_only=True`. Si aun así hay fills taker, el
motor decidió que la orden era agresiva (libro cruzado o clamp insuficiente).
El modo `cross_spread` manda límites en el lado opuesto del BBO a propósito,
para que la comparación maker vs cruzar **no sea tautológica**.

## La muestra es pequeña y OFI no predice

- ~40 min, **un** símbolo (BTCUSDT USD-M), un régimen.
- OFI L1 **lead-1** R² ≈ 0–1.4 %; **contemporáneo** ≈ 22–29 % (1s/5s/10s).
  Eso ya se midió. Un MM que sesga por el *signo* de OFI está reaccionando a
  un flujo que se mueve **con** el mid, no que lo adelanta.
- Si el backtest gana dinero, no has descubierto alpha. Si lo pierde, no has
  falsado OFI como feature de estado. Has probado el adaptador.

## Estrategia: test de pipeline, no un diseño serio

- Un bid y un ask, 0.001 BTC, cancel/replace en cada batch de depth
  (~100 ms). En vivo eso sería spam de órdenes; aquí el risk engine está en
  `bypass=True` para no ahogar el replay.
- Sesgo binario (`±1` tick si `|suma e_n 1s|` supera el umbral). Sin
  inventario como término (salvo lo que el fill model deje abierto al corte).
- **No** se cierran posiciones con market al `on_stop` (contaminaría maker vs
  taker). El PnL no realizado queda marcado al final de la muestra.

## Lo que no debes leer en la tabla de resultados

- “El MM sesgado por OFI funciona / no funciona en BTC.”
- “0.02% maker es la tarifa que verías en cuenta real.” (VIP, descuentos BNB,
  y el maker/taker *real* de cada fill dependen del matching, no de lo que
  pretendíamos ser.)
- “Fill rate X% es el que tendrías en el exchange.” Es el fill rate **del
  simulador L2**.
- **541 trades con qty 0** en la captura se descartan: nautilus 1.231 exige
  `TradeTick.size > 0`.

## Lo observado en la corrida de 39.6 min

- Maker post-only: **0 fills taker**, 1874 maker. El flag `post_only` hizo lo
  que dice. PnL −72 USDT, de los cuales ~29 son fees.
- Crossing: ~43k fills taker, fees ~1333 USDT, PnL −1335 USDT. Cruzar el spread
  **destruyó** el PnL en esta muestra, casi entero por comisiones, no por el
  signo de OFI.
- Fill rate maker 29 % vs crossing 100 %: el simulador sí rellena límites
  pasivos cuando el tape transa a través, y rellena *todos* los agresivos.
  Eso no es la cola de Binance.

Si necesitas un backtest que pretenda ser operativo: latencia medida,
prioridad L3 (no solo ProbQueue sobre L2), muestra de días/semanas,
costes de funding, y una hipótesis que no contradiga el R² lead-1 ≈ 0.
hftbacktest cubre el hueco “modelo de cola HFT sobre el mismo Parquet”;
**no** cubre competidores, impacto ni L3. Eso **no** es una fase de ejecución.
