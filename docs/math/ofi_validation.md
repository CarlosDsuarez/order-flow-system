# Validación empírica de OFI (Cont-Kukanov-Stoikov 2014, L1)

Generado: **2026-09-02 16:22:04Z** en `Carloair.local`.
Script: `scripts/validate_ofi.py` (fuente de verdad; el notebook solo lo documenta).

## Setup

- Exchange: `binance_futures` (Binance USD-M, mainnet `fapi` / `fstream`)
- Símbolo: `BTCUSDT`
- Directorio: `/Users/carlos_suarez7/ORDER FLOW/data/live-btcusdt-45min`
- Duración de la serie L1: **2375.7 s (~39.6 min)** (pedida 2700 s / 45 min).
  El recorder murió antes del `finally`; `capture_meta.json` se reconstruyó después
  desde Parquet + logs. Cumple el mínimo de 30 min.
- Estados L1 sincronizados: 19667; eventos $e_n$: 19664; épocas (resyncs de libro): 3
- Updates L1 / s: 8.28; deltas/s (storage): 8.28
- Latencia observada (recv - event, log en vivo, n=10000 acotados): media **0.059 s**, p99 **0.182 s**
- Gaps event-time (>200 ms): 1. Reconnects WS (close 1006 + handshake timeout); 3 épocas.
- Dual sockets: no (combined `?streams=btcusdt@trade/btcusdt@depth@100ms`)
- Trades: **146394** persistidos (~61.6 / s); CVD terminal = `124.9360`.
  El WS `@aggTrade` sigue silencioso; `@trade` (mismo `m`) llenó `trades/`.

Tick de BTCUSDT USD-M: **0.1 USD**. Δmid se reporta en **precio** (USD), no en ticks.

## Especificación

Muestreo: cada snapshot/delta aplicado con BBO válido (diffs `@depth@100ms`, no L3).
`e_n` es la formula L1 de Cont et al. section 2.1 (ver `docs/math/ofi.md`).
Ventana \(k\): \([t_0 + k\tau,\ t_0 + (k+1)\tau)\), \(\tau \in \{1,5,10\}\) s.
\(\mathrm{OFI}_k = \sum e_n\) con \(\tau_n\) en la ventana.
Mid al cierre de barra: último mid L1 con timestamp en la barra, carry-forward.
Se descartan barras con época mezclada (resync) o mid no finito.

**Lead-1 (lo que pidió Carlos, predictivo):**

\[\Delta \mathrm{mid}_{k+1} = \alpha + \beta\,\mathrm{OFI}_k + \varepsilon_{k+1}\]

donde \(\Delta\mathrm{mid}_{k+1} = \mathrm{mid}_{k+1} - \mathrm{mid}_k\).

**Contemporánea (lo que estima el paper, eq. (4)):**

\[\Delta \mathrm{mid}_k = \alpha + \beta\,\mathrm{OFI}_k + \varepsilon_k\]

OLS con errores HAC Newey-West, lags \(= \lfloor 4(n/100)^{2/9}\rfloor\).

## Resultados - lead-1 (siguiente ventana)

| τ | n | β | EE HAC | t | R² | lags NW | % barras inválidas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1s | 2376 | 0.0491 | 0.0070 | 7.0044 | 0.0142 | 8 | 0.00 |
| 5s | 475 | 0.0240 | 0.0134 | 1.7892 | 0.0037 | 5 | 0.00 |
| 10s | 237 | -0.0107 | 0.0204 | -0.5247 | 0.0007 | 4 | 0.00 |

## Resultados - contemporánea (misma ventana, como el paper)

| τ | n | β | EE HAC | t | R² | lags NW | % barras inválidas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1s | 2376 | 0.1935 | 0.0068 | 28.2926 | 0.2204 | 8 | 0.00 |
| 5s | 475 | 0.2085 | 0.0124 | 16.8351 | 0.2817 | 5 | 0.00 |
| 10s | 237 | 0.2107 | 0.0138 | 15.3224 | 0.2852 | 4 | 0.00 |

## Comparación con Cont, Kukanov & Stoikov (2014)

El paper reporta **R² medio ≈ 65 %** en la especificación **contemporánea** a **10 s**, en US equities TAQ, 50 S&P 500 names, April 2010. No es un test de ΔP de la siguiente ventana.
Un footnote avisa tautología: eventos que *mueven* el quote entran en OFI; excluyéndolos el R² bajó a 35-60 % y siguió siendo alto.

Este experimento es más duro: perpetuo crypto, un símbolo, diffs L2 a 100 ms (no TAQ L1 tick-a-tick), y la columna lead-1 pide predicción, no explicación.
**No esperes ~65 %.** Si el R² contemporáneo a 10 s ya es mucho menor, el gap es venue + muestreo + horizonte, no un bug de e_n (los unit tests cubren la casuística del paper).

## Conclusión

OFI es **principalmente contemporáneo** en este setup (explica el Δmid de la misma ventana, no la siguiente). Como input **predictivo** para market making pasivo es débil: no uses β de la siguiente barra como si fuera el 65 % del paper.

## Caveats

- 30-60 min es corto frente a un mes de TAQ en 50 nombres.
- Un símbolo (BTCUSDT) vs 50 acciones US.
- Crypto perpetuo 24/7 vs equity RTH; tick 0.1 USD vs 0.01 USD.
- Reloj: `ts_event` del exchange vs recepción local; p99 de latencia arriba.
- Libro L2 agregado 100 ms, no el L3 del paper.
- Barras con resync se tiran; un burst de gaps reduce n.

## Reproducción

```bash
uv run --extra notebooks python scripts/validate_ofi.py \
  --root "data/live-btcusdt-45min" --report docs/math/ofi_validation.md
```
