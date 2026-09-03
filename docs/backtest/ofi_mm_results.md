# Backtest OFI-MM (pipeline, no una ventaja)

Generado: **2026-09-02 19:21:26Z**.
Script: `scripts/run_ofi_mm_backtest.py`. Motor: **nautilus_trader 1.231.0** (estable PyPI; no 2.0.0rc).

> **Esto no es evidencia de un edge de OFI.** En esta captura el R² lead-1 de OFI L1 es ~0-1.4 % y el contemporáneo ~22-29 % ([docs/math/ofi_validation.md](../math/ofi_validation.md)). Un PnL positivo o negativo aquí solo prueba que datos → métrica → orden simulada está cableado. Limitaciones: [docs/backtest_limitations.md](../backtest_limitations.md).

## Captura y parámetros

- Directorio: `/Users/carlos_suarez7/ORDER FLOW/data/live-btcusdt-45min`
- Exchange / símbolo: `binance_futures` / `BTCUSDT`
- Instrumento nautilus: `BTCUSDT-PERP.BINANCE` (tick 0.1, size 0.001)
- Duración de la serie (min-max `ts_event`): **2376.2 s** (~39.6 min)
- Batches de libro: 21622; trades públicos: 145853 (541 prints con `qty=0` no se
  enviaron a nautilus: `TradeTick` exige size positivo)
- Fees: maker 0.020% / taker 0.040% (Binance USD-M típico ~0.02% / ~0.04%)
- Spread: 2 ticks alrededor del mid, luego sesgo OFI ±1 tick si `|OFI_1s| > 5.0`
- OFI: suma móvil de `e_n` (Cont et al., `OfiAccumulator`) en 1 s
- Tamaño: 0.001 BTC por lado; post-only GTC salvo crossing
- Capital inicial: 100000 USDT, apalancamiento 1x, sin modelo de latencia ni liquidación

OFI positivo ⇒ cotizaciones **arriba** (bid y ask más altos). OFI negativo ⇒ abajo.

## Resultados

| Escenario | PnL total USDT | Realizado | No realizado | Fees | Órdenes enviadas | Órdenes con fill | Eventos fill | Maker fills | Taker fills | Fill rate | Canceladas | Rechazadas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Maker post-only | -71.9350 | -71.2730 | -0.6620 | 28.8834 | 6513 | 1874 | 1874 | 1874 | 0 | 28.77% | 4637 | 0 |
| Crossing (límite agresivo) | -1335.3015 | -1335.3011 | -0.0004 | 1332.6191 | 43242 | 43240 | 43240 | 2 | 43238 | 100.00% | 2 | 0 |

**Crossing empeoró el PnL** respecto al maker post-only en esta muestra (hipótesis: cruzar el spread mata la estrategia). Sigue sin ser un test de edge de OFI: es un test de fees + fill model.

## Cómo se alimentó el motor

Parquet hive `snapshots/` + `deltas/` + `trades/` → `capture_to_ops` (CLEAR+ADD por snapshot; qty 0 → DELETE; qty > 0 → UPDATE) → `OrderBookDeltas` / `TradeTick`. `BacktestEngine.add_data` por tipo, luego `sort_data()`. Venue `BINANCE`, `BookType.L2_MBP`, `trade_execution=True`, `queue_position=True`, `liquidity_consumption=True`.

## Reproducción

```bash
uv sync --extra backtest
uv run python scripts/run_ofi_mm_backtest.py \
  --root "/Users/carlos_suarez7/ORDER FLOW/data/live-btcusdt-45min" --report docs/backtest/ofi_mm_results.md
```
