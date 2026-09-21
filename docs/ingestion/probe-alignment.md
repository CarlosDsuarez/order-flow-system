# Serie de probes alineados — BTCUSDT 2026-09-21

Diagnóstico offline (sin freeze del vivo): captura 180 s con
`record_l2.py --rest-probe-every 15` (13 probes) + `audit_probe_alignment.py`
(reconstrucción Parquet al primer `u >= R` + `compare_top_levels` top-20).
Protocolo de captura sano: 0 gaps, 0 resyncs, 0 reconnects; reconstruct
final live == replay (`...892` == `...892`), libro no cruzado.

## Serie (`t` = event time aprox, rate = mismatches/40)

| probe | t (UTC aprox) | rate | max Δqty | delta_ids | batches | veredicto |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 15:20:03 | 0.000 | 0.000 | 1765 | 8 | PASS |
| 1 | 15:20:18 | 0.025 | 0.001 | 529 | 3 | PASS |
| 2 | 15:20:34 | 0.075 | 0.407 | 3780 | 8 | PASS |
| 3 | 15:20:50 | 0.100 | 0.789 | 5465 | 2 | PASS (= warn, `>` no dispara) |
| 4 | 15:21:06 | 0.125 | 3.281 | 3174 | 6 | **FAIL** |
| 5 | 15:21:21 | 0.400 | 3.356 | 20517 | 9 | **FAIL** |
| 6 | 15:21:37 | 0.050 | 0.171 | 9440 | 7 | PASS |
| 7 | 15:21:52 | 0.300 | 0.885 | 6668 | 7 | **FAIL** |
| 8 | 15:22:08 | 0.100 | 0.722 | 6981 | 2 | PASS (= warn) |
| 9 | 15:22:24 | 0.000 | 0.000 | 845 | 5 | PASS |
| 10 | 15:22:39 | 0.350 | 1.412 | 8015 | 10 | **FAIL** |
| 11 | 15:22:55 | 0.000 | 0.000 | 0 | 4 | PASS |
| 12 | 15:23:08 | — | — | — | — | stale (R más allá de la cinta) |

**4/13 probes superan 10 % → la serie FAIL** (exit 1 del auditor). Sin suavizar.

## Lectura (sin anticipar veredicto de arreglo)

* **No es sesgo permanente**: probes 0, 9 y 11 dan **0.000 exacto** — cuando el
  libro coincide, el método lo muestra. La mediana es 0.0875 (~91 % match).
* **No es un spike aislado**: 4 breaches en 3 min (0.125–0.40, max Δ hasta
  3.356). El 7/40 del live encaja en este patrón, no era anomalía única.
* Los breaches correlacionan con `delta_ids` grande (overshoot de batch gordo:
  hasta 20517 IDs dentro de un solo batch de 100 ms). El catch-up en batches
  previos a R es inocuo; lo que discrepa es el tramo intra-batch final.
  Hipótesis abiertas: (a) movimiento real top-20 dentro del batch gordo
  (overshoot metodológico irreducible con batches atómicos); (b) divergencia
  transitoria real en ráfagas. Esta serie no las separa.
* El honesty end-of-run del recorder (método viejo, libro caliente: 15/40,
  max Δ 9.423, REST por delante) queda supersedido por esta serie alineada.

## Cómo repetir

```bash
uv run python scripts/record_l2.py --symbol BTCUSDT --seconds 180 \
  --out data/probe_run_XXXX --rest-probe-every 15 --rest-probe-levels 20
uv run python scripts/audit_probe_alignment.py --tape data/probe_run_XXXX --levels 20
```
