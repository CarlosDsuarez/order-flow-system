# nautilus vs hftbacktest — corrección de fill rate por modelo de cola

**Fecha:** 2026-09-02 20:54:48Z (corrida hftbacktest).
**Dataset:** `data/qa-audit-15min` (el de la auditoría QA; **no** el set de ~40 min).
**Estrategia:** la misma economía OFI-MM (un bid + un ask, 0.001 BTC, mid ± 2 ticks, ±1 si `|OFI_1s|>5`, OFI positivo sesga arriba, fees 0.02 % / 0.04 %). OFI = `OfiAccumulator` / `RollingOfi`; **no** se reimplementa \(e_n\).

> **Esto no es un edge de OFI.** Lead-1 R² sigue siendo débil. La pregunta de esta nota es más estrecha: *¿cuánto infló nautilus el fill rate al estimar la cola L2 de otra forma?*

Detalle del algoritmo hftbacktest: [hftbacktest_queue.md](hftbacktest_queue.md).
Números nautilus 15 min: [qa_audit_report.md](../qa_audit_report.md) y `data/qa-audit-15min/ofi_mm_results.md`.
Números hftbacktest: [ofi_mm_hftbacktest_results.md](ofi_mm_hftbacktest_results.md).
GO/NO-GO: [go_no_go_decision.md](../go_no_go_decision.md).

## Los dos fill models (manzanas con manzanas)

| | nautilus 1.231.0 (auditoría 15 min) | hftbacktest 2.4.4 |
| --- | --- | --- |
| Extra | `backtest` | `hftbacktest` (separado) |
| Adapter | `capture_to_ops` → `OrderBookDeltas` + `TradeTick` | `capture_to_hft_feed` → NumPy `event_dtype` (**segundo** adapter; no reutiliza el de nautilus) |
| Libro | L2_MBP | `HashMapMarketDepthBacktest` L2 |
| Cola | `queue_position=True`: FIFO a partir del size **mostrado**; UPDATE recorta; DELETE limpia. Primo de `RiskAverseQueueModel`. | `ProbQueueModel` + `PowerProbQueueFunc(n=2)` (`power_prob_queue_model(2.0)`). Las bajas de qty se parten delante/detrás con \(P \propto x^2\). |
| Trades | `trade_execution=True`, `liquidity_consumption=True` | `TRADE_EVENT`; fill maker al frente de cola si el print pega el precio (`NoPartialFillExchange`) |
| Snapshots periódicos | **H2:** se enviaron 885 CLEAR+ADD (skip **0**/884) | **884 omitidos** a propósito (un CLEAR/s resetearía la cola) |
| Trades qty=0 | 181 dropped | 181 dropped |
| Conservación | 9686 book batches, 30436 trades, Δ=0 | initial=2000 niveles; incremental=964797; skip 884; Δ=0 |
| Latencia de orden | ninguna (`latency_model=None`) | `constant_order_latency(0, 0)`; `local_ts = max(recv, exch+1)` |
| Post-only | `post_only=True` (GTC) | TIF `GTX` |
| Cadencia de requote | cada batch de depth (incl. snapshots) | `elapse(100 ms)` si cambió L1 |

L2 en ambos: **no** hay `order_id` de competidores, ni icebergs, ni la cola real de Binance.

## Maker post-only (el caso que importa para la cola)

| Métrica | nautilus (FIFO mostrado + trade-execution) | hftbacktest (`ProbQueueModel` + `PowerProbQueueFunc(n=2)`) | Δ (hft − nautilus) |
| --- | ---: | ---: | ---: |
| Fill rate | **26.38 %** | **27.72 %** | **+1.34 pp** |
| Órdenes enviadas | 1308 | 1569 | +261 |
| n fills (órdenes con fill) | 345 | 435 | +90 |
| Eventos fill | 345 | 435 | +90 |
| Maker / taker | **345 / 0** | **435 / 0** | +90 / 0 |
| PnL total USDT | **−8.2596** | **−12.0937** | **−3.8341** |
| Realizado | −7.0161 | −0.9734 | +6.0427 |
| No realizado | −1.2436 | −4.3944 | −3.1508 |
| Fees | 5.3343 | 6.7258 | +1.3915 |
| Canceladas | 961 | 1131 | +170 |
| Rechazadas | 0 | 1 | +1 (GTX) |
| PnL / min | −0.55 | −0.81 | −0.26 |

**La diferencia de fill rate *es* la corrección de realismo.** Aquí es **pequeña** (~1.3 pp, ~5 % relativo). nautilus **no** sobreestimó el fill rate frente al modelo probabilístico de hftbacktest: si acaso el FIFO+clip es un poco **más** conservador (345 vs 435 fills; coherente con “las cancelaciones no te adelantan”).

Por qué un Δ chico es un hallazgo válido, no un fallo:

1. nautilus 1.231 **ya** estimaba cola (`queue_position=True`). No era “fill al toque”.
2. `ProbQueueModel` solo cambia *cómo* se parten las bajas de size; el throttle L2 a **100 ms** sigue dominando qué ves del libro.
3. El tape es el mismo (30436 prints, 8801 diffs). Sin MBO, los dos modelos adivinan la misma cantidad mostrada.
4. Hay un confounder de feed: nautilus *sí* ingiere CLEAR+ADD 1 Hz (H2); hftbacktest los omite para no resetear la cola. Eso mueve el denominador (1308 vs 1569 submits) y puede alargar la vida de una orden en cola. Aun así el fill rate se queda en la misma banda 26–28 %.

Más fills en hftbacktest **no** mejoran el PnL: fees +1.39 USDT e inventario marcado peor (no realizado −4.39 vs −1.24). El número que debe pesar para go/no-go es el **PnL con cola probabilística: −12.09 USDT**, no el −8.26 de nautilus.

## Crossing (take; la cola casi no aplica)

| Métrica | nautilus | hftbacktest | Δ |
| --- | ---: | ---: | ---: |
| Fill rate | 100.00 % | 99.99 % | −0.01 pp |
| n fills | 19372 (0 maker / 19372 taker) | 14449 (0 maker / 14449 taker) | −3923 |
| PnL USDT | **−599.9731** | **−447.3481** | +152.63 |
| Fees | 599.0481 | 446.8118 | −152.24 |
| Órdenes enviadas | 19372 | 14450 | −4922 |

Crossing **sigue siendo fee-dominado** (fees ≈ |PnL|). Menos submits/fills en hftbacktest (cadencia 100 ms + skip de snapshots, vs un requote por cada batch nautilus) ⇒ menos taker fee, PnL menos malo, **sigue siendo un desastre** frente al maker (−447 vs −12). La cola probabilística no es el mecanismo: el take se llena al best (`NoPartialFillExchange`). Se reporta para que el go/no-go siga comparable con el prompt 5 / la QA.

## Qué implica (y qué no)

- **No** “nautilus mentía un 26 % de fills que en realidad serían ~0”. El 26 % aguanta bajo un modelo de cola *más* HFT.
- **No** hay un edge escondido: maker sigue en pérdida; con ProbQueue la pérdida es **mayor**.
- **No** autoriza testnet ni capital. Ver la sección nueva en [go_no_go_decision.md](../go_no_go_decision.md).
- Sigue sin competidores reales, sin impacto, sin L3, sin latencia de red. [backtest_limitations.md](../backtest_limitations.md).

## Reproducción

```bash
uv sync --extra hftbacktest
uv run python scripts/run_ofi_mm_hftbacktest.py \
  --root "data/qa-audit-15min" --report docs/backtest/ofi_mm_hftbacktest_results.md
```

nautilus (ya corrido en la QA; no hace falta repetirlo para esta tabla):

```bash
uv sync --extra backtest
uv run python scripts/run_ofi_mm_backtest.py \
  --root "data/qa-audit-15min" --report data/qa-audit-15min/ofi_mm_results.md
```

Los dos extras pueden coexistir: `uv sync --extra backtest --extra hftbacktest`.
