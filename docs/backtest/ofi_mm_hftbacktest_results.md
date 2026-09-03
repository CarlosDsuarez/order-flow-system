# Backtest OFI-MM con cola hftbacktest (pipeline, no una ventaja)

Generado: **2026-09-02 20:54:48Z**.
Script: `scripts/run_ofi_mm_hftbacktest.py`. Motor: **hftbacktest 2.4.4**. Modelo de cola: **ProbQueueModel+PowerProbQueueFunc(n=2)**.

> **Esto no es evidencia de un edge de OFI.** Comparación con nautilus: [queue_position_comparison.md](queue_position_comparison.md). Limitaciones: [docs/backtest_limitations.md](../backtest_limitations.md), [docs/backtest/hftbacktest_queue.md](hftbacktest_queue.md).

## Captura y parámetros

- Directorio: `/Users/carlos_suarez7/ORDER FLOW/data/qa-audit-15min`
- Exchange / símbolo: `binance_futures` / `BTCUSDT`
- Tick 0.1 / lote 0.001 (igual que nautilus BTCUSDT-PERP)
- Duración de la serie (min-max `exch_ts` del feed incremental): **898.8 s** (~15.0 min)
- Snapshots in: 885; periódicos omitidos: 884; resyncs: 0
- Eventos incrementales: 964797; trades públicos: 30436; qty=0 dropped: 181
- Conservación adapter Δ: **0**
- Fees: maker 0.020% / taker 0.040%
- Spread: 2 ticks alrededor del mid, sesgo OFI ±1 tick si `|OFI_1s| > 5.0`
- OFI: suma móvil de `e_n` (Cont et al., `OfiAccumulator`) en 1 s
- Tamaño: 0.001 BTC por lado; GTX post-only salvo crossing (GTC)
- Latencia de orden: 0 ns (comparable a nautilus sin `latency_model`)

OFI positivo ⇒ cotizaciones **arriba**. OFI negativo ⇒ abajo.

## Resultados

| Escenario | PnL total USDT | Realizado | No realizado | Fees | Órdenes enviadas | Órdenes con fill | Eventos fill | Maker fills | Taker fills | Fill rate | Canceladas | Rechazadas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Maker post-only (GTX) | -12.0937 | -0.9734 | -4.3944 | 6.7258 | 1569 | 435 | 435 | 435 | 0 | 27.72% | 1131 | 1 |
| Crossing (límite agresivo GTC) | -447.3481 | -0.5363 | -0.0001 | 446.8118 | 14450 | 14449 | 14449 | 0 | 14449 | 99.99% | 1 | 0 |

**Crossing empeoró el PnL** respecto al maker post-only en esta muestra (hipótesis: cruzar el spread mata la estrategia). Sigue sin ser un test de edge de OFI: es un test de fees + fill model de cola.

## Cómo se alimentó el motor

Parquet hive → `capture_to_hft_feed` (segundo adapter, **no** `OrderBookDeltas`): primer snapshot = `initial_snapshot` (`DEPTH_SNAPSHOT_EVENT`); diffs = `DEPTH_EVENT` (qty 0 borra); trades = `TRADE_EVENT` (BUY/SELL = agresor). Snapshots periódicos con el mismo `last_update_id` se omiten para no resetear la cola. `HashMapMarketDepthBacktest`, `no_partial_fill_exchange`, `power_prob_queue_model(2.0)` = ProbQueueModel+PowerProbQueueFunc(n=2).

## Reproducción

```bash
uv sync --extra hftbacktest
uv run python scripts/run_ofi_mm_hftbacktest.py \
  --root "/Users/carlos_suarez7/ORDER FLOW/data/qa-audit-15min" --report docs/backtest/ofi_mm_hftbacktest_results.md
```
