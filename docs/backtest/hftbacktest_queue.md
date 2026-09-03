# hftbacktest 2.4.4 — formato de feed y modelo de cola

**Recuperado:** 2026-09-02.
**Paquete:** `hftbacktest==2.4.4` (PyPI, última estable a esa fecha; publicada 2025-12-10). Extra separado: `uv sync --extra hftbacktest` (no está en el extra `backtest` de nautilus).
**Código:** `order_flow.backtest.hft_adapter` (Parquet → arrays) y `order_flow.backtest.hft_runner` (OFI-MM). **No** hay capa live.

Fuentes oficiales leídas el 2026-09-02 (no adivinar la API):

| Documento | URL |
| --- | --- |
| Order Fill (exchange + queue models) | https://hftbacktest.readthedocs.io/en/latest/order_fill.html |
| Probability Queue Position Models (tutorial) | https://hftbacktest.readthedocs.io/en/latest/tutorials/Probability%20Queue%20Models.html |
| Data / Data Preparation | https://hftbacktest.readthedocs.io/en/latest/data.html |
| Initialization (`BacktestAsset`) | https://hftbacktest.readthedocs.io/en/latest/reference/initialization.html |
| Rust `backtest::models` (crate 0.9.4, el núcleo de 2.4.4) | https://docs.rs/hftbacktest/0.9.4/hftbacktest/backtest/models/ |
| GitHub | https://github.com/nkaz001/hftbacktest |

Artículos que el propio Order Fill cita como origen del modelo probabilístico:

- [quant.stackexchange #3782](https://quant.stackexchange.com/questions/3782/how-do-we-estimate-position-of-our-order-in-order-book)
- [rigtorp 2013, Estimating order queue position](https://rigtorp.se/2013/06/08/estimating-order-queue-position.html)
- Notas de Almgren (PIMS): http://www.math.ualberta.ca/~cfrei/PIMS/Almgren5.pdf

## Por qué un segundo adapter

nautilus 1.231 consume `OrderBookDelta` / `OrderBookDeltas` + `TradeTick` (`order_flow.backtest.conversion`).
hftbacktest 2.4.4 consume un **array estructurado NumPy** de 64 bytes (`align=True`):

```
(ev u8, exch_ts i8, local_ts i8, px f8, qty f8, order_id u8, ival i8, fval f8)
```

Definido en `hftbacktest.types.event_dtype`. **No** es el objeto nautilus. El segundo adapter (`hft_adapter.py`) **no importa** `nautilus_trader` ni `hftbacktest`: copia las flags de `hftbacktest.types` 2.4.4 para que `uv sync` sin extras siga verde.

Flags (kind = `ev & 0xFF`; **no** uses `ev & DEPTH_EVENT` para filtrar, porque `DEPTH_CLEAR_EVENT=3` solapa el bit de DEPTH):

| Constante | Valor | Uso aquí |
| --- | ---: | --- |
| `DEPTH_EVENT` | 1 | Diff incremental. `qty == 0` borra el nivel. |
| `TRADE_EVENT` | 2 | Print público. `BUY_EVENT` / `SELL_EVENT` = agresor. |
| `DEPTH_CLEAR_EVENT` | 3 | Resync: vaciar hasta un precio. |
| `DEPTH_SNAPSHOT_EVENT` | 4 | Primer REST (y niveles de un resync). |
| `EXCH_EVENT` | `1<<31` | Procesar en el reloj del exchange. |
| `LOCAL_EVENT` | `1<<30` | Procesar en el reloj local. |
| `BUY_EVENT` / `SELL_EVENT` | `1<<29` / `1<<28` | Bid vs ask, o agresor compra vs venta. |

`local_ts` debe ser **estrictamente mayor** que `exch_ts` (validación oficial). Si el `recv` de la captura va atrasado por el reloj, el adapter usa `exch_ts + 1`. Eso **no** inyecta los ~200 ms de skew de la captura: es comparable a nautilus sin `latency_model`.

Layout que espera la librería (Data Preparation):

1. Primer snapshot REST → `initial_snapshot` como `DEPTH_SNAPSHOT_EVENT` (el libro arranca vacío).
2. Diffs → `DEPTH_EVENT`.
3. Trades → `TRADE_EVENT`. A igual `exch_ts`, trades **antes** que depth.
4. Snapshots periódicos con el mismo `last_update_id` que el libro vivo → **omitidos**. Un CLEAR+SNAPSHOT cada segundo **resetearía la cola estimada**. Un snapshot con id **nuevo** es resync: `DEPTH_CLEAR` y luego `DEPTH_SNAPSHOT`.

Drops intencionales (un Δ de conservación ≠ 0 es fallo de test): trades con `qty=0`, niveles de snapshot con `qty=0`, deltas vacíos, snapshots periódicos duplicados.

## Qué algoritmo de cola usa esta corrida

Este proyecto fija:

```text
BacktestAsset.power_prob_queue_model(2.0)
= ProbQueueModel + PowerProbQueueFunc(n=2)
```

Es el mismo “SquareProbQueueModel” del tutorial oficial de BTCUSDT (`power_prob_queue_model(2)`).

**Qué hace `ProbQueueModel` (docs Order Fill, 2026-09-02):**
con L2 no hay `order_id` nuestro en el tape. Cuando el tamaño *mostrado* del nivel baja (cancel / modify, no un trade), hay que **adivinar** si esa cantidad desapareció *delante* o *detrás* de nosotros. El modelo asigna

\[
P(\text{la baja ocurre detrás}) = \frac{f(\text{back})}{f(\text{back})+f(\text{front})}
\]

y avanza la posición con el complemento. `PowerProbQueueFunc(n)` usa \(f(x)=x^n\). Con **n=2**, \(f(x)=x^2\).

Condiciones de borde que exige la doc: \(P=0\) si estamos en la cabeza (toda baja es detrás); \(P=1\) si estamos en la cola (toda baja es delante).

**Qué no es:**

| Modelo 2.4.4 | Comportamiento | ¿Lo usamos? |
| --- | --- | --- |
| `RiskAverseQueueModel` (`risk_adverse_queue_model`; el nombre en Rust lleva *Adverse*) | Lo más conservador: las bajas de qty ocurren **solo en la cola**; la posición avanza **solo con trades** al precio. | No. Es el primo más cercano al FIFO+clip de nautilus. |
| `ProbQueueModel` + `LogProbQueueFunc` / `LogProbQueueFunc2` | \(f=\log(1+x)\); el perfil **cambia** con el size total del nivel. | No. |
| `power_prob_queue_model2` / `power_prob_queue_model3` | Otras potencias / perfiles (tutorial). | No. |
| L3 (`ADD_ORDER` / `CANCEL_ORDER` + `order_id`) | Cola real si el exchange publica MBO. Binance USD-M público es L2. | No hay tape L3. |

Exchange model: `no_partial_fill_exchange` (el default). Un maker se llena entero si (a) el BBO cruza el límite, (b) un trade *travesó* el precio, o (c) estamos al frente de la cola **y** el trade pega exactamente en nuestro precio. Un take se llena entero al best **sin** mirar el size (doc: irreal si el size es grande; aquí el lote es 0.001 BTC).

**Lo que L2 no puede saber:** identidad de cada orden, icebergs, STP, qué cancel concreto es delante vs detrás, prioridad entre *peers* makers. El modelo probabilístico es una hipótesis sobre el size mostrado, no la cola de Binance.

## Comparar con nautilus 1.231 sin eslóganes

nautilus **sí** modela cola: `queue_position=True` + `trade_execution=True` + `liquidity_consumption=True` (L2_MBP). Estima FIFO a partir del size *mostrado* al aceptar la orden; un DELETE del nivel limpia; un UPDATE recorta la cola al nuevo size. Eso se parece más a **`RiskAverseQueueModel`** (las cancelaciones no te adelantan) que a `ProbQueueModel`.

Decir “nautilus sin cola vs hftbacktest con cola” es **falso**. Lo correcto: FIFO+clip (nautilus) vs ProbQueue \(x^2\) (hftbacktest). El segundo es el modelo *más específico de HFT* de los dos; no es magia L3.

Latencia de orden en ambas corridas: **0**. El adapter hft fuerza `local_ts > exch_ts` sin meter el delay de recv de la captura.

## Motor

`HashMapMarketDepthBacktest` (depth sparse; BTCUSDT no cabe en un ROI denso de ticks a 0.1). Fees `trading_value_fee_model(0.0002, 0.0004)`. Tick 0.1, lote 0.001. `constant_order_latency(0, 0)`.

**No** se usa `HashMapMarketDepthLiveBot` ni ningún path de ejecución. El extra de PyPI puede traer bindings live; este repo no los llama.
