# Auditoría QA (prompts 0–6)

**Fecha:** 2026-09-02 (captura 20:11:31Z–20:26:47Z, elapsed 916.0 s).
**Máquina:** `Carloair.local`. Red de investigación doméstica. **Sin órdenes.**
**Dataset independiente:** `data/qa-audit-15min` (no se reutilizó `data/live-btcusdt-45min`).
**Pytest (tras instrumentación opt-in):** 276 passed, 4 skipped, coverage **93.54 %**.

## Resumen ejecutivo

**Sí se puede construir encima como plataforma de investigación** (métricas, replay, más capturas, experimentos de microestructura). **No hay un fallo estructural de integridad del libro** en esta corrida: 0 gaps de secuencia, 0 resyncs, 0 reconnects, proceso vivo 15 min, reconstrucción al final y a 5 timestamps aleatorios coherente, streaming ≡ batch, conservación del adaptador nautilus explicada.

Eso **no** revoca el **NO-GO de ejecución en vivo** ya decidido en [`docs/go_no_go_decision.md`](go_no_go_decision.md). Esta auditoría es otra pregunta: *¿la tubería de datos es honesta para seguir investigando?* **GO investigación / NO-GO live** (incluido testnet).

Hay **inestabilidad numérica** de OFI/MLOFI entre la muestra ~40 min y esta de 15 min (R² contemporáneo 22–29 % → 48–69 %; lead-1 a 10 s cambia de signo). No es un bug de `e_n` (stream ≡ batch en este tape). Es muestra corta + régimen. No uses una sola ventana de 15–40 min como constante.

| Severidad | N |
| --- | ---: |
| **CRÍTICO** | **0** |
| **IMPORTANTE** | 4 |
| **MENOR** | 6 |

**Veredicto investigación:** **GO** para capas de investigación encima de captura → Parquet → métricas → replay. Corregir los IMPORTANTE **antes** de tratar el adaptador nautilus o `capture_meta.json` como fuente de verdad de latencia / de “snapshots periódicos omitidos”.

**Veredicto ejecución:** sigue **NO-GO** (latencia p99 ajustada de depth **259 ms** en esta corrida sin reconnects; auditoría previa **783 ms** con 3 reconnects; R² lead-1 débil).

## Hallazgos

| id | fase | severidad | componente | evidencia |
| --- | --- | --- | --- | --- |
| H1 | D | IMPORTANTE | OFI/MLOFI R² | Contemporáneo 1s/5s/10s: 22.0/28.2/28.5 % (40 min) → **48.4/60.7/68.9 %** (15 min). Lead-1 10 s: β −0.0107 (R² 0.07 %) → β **+0.0660** (R² **6.06 %**), **cambio de signo**. n₁₀ₛ=90. Script marca `insufficient: true` (<30 min). Stream≡batch. |
| H2 | E | IMPORTANTE | `capture_to_ops` | Docstring en `src/order_flow/backtest/conversion.py:182-199` promete omitir snapshots periódicos con el mismo `last_update_id`. En este tape se omitieron **0/884**. Nautilus recibió **885 CLEAR+ADD** + 8801 deltas = 9686 batches. Sort `(ts, kind=0 snap, uid)` pone el snapshot **antes** del delta co-timestamped. Reconstruct **sí** omite (`src/order_flow/storage/reconstruct.py:115-124`). Qty L2 absoluta ⇒ probable idempotencia; **no se corrigió**. |
| H3 | B | IMPORTANTE | `FeedStats` latencia | `MAX_LATENCY_SAMPLES = 10_000` (`src/order_flow/ingestion/binance_futures.py:74`, `record_latency` L291-294). `capture_meta.json` reporta n=10000, p99 cruda **+63.3 ms**. Parquet completo n=**40303**, p99 cruda **+153.1 ms**, p99 ajustada **+338.5 ms**. Quien lea el meta subestima la cola. |
| H4 | C | IMPORTANTE | Libro vs REST | Sidecar durante captura (sondas 0–4, mismo `lastUpdateId` bracket): **95.0–97.5 %** de 40 niveles. Mismatches residuales 1–2/40, \|Δqty\| máx 0.078. Binance no publica checksum; el `u` del WS cubre un rango y el REST `lastUpdateId` cae dentro. **No** es divergencia no detectada por tests. Sondas 5–6 **después** de parar el feed: 65 % / 57.5 % — carrera temporal, no corrupción del Parquet. |
| H5 | A | MENOR | coverage | Ningún módulo medido en **0 %** ni **<50 %**. El más bajo incluido: `ingestion/live.py` **59 %**. `backtest/ofi_mm.py` y `backtest/runner.py` están en `omit` de coverage: **no medidos**. |
| H6 | B | MENOR | Reloj | Offset medio `local−server` **−185.4 ms** (n=15, RTT medio 416 ms). Cruda negativa **no** es ventaja. Alineado con la auditoría previa (−196.7 ms). |
| H7 | C | MENOR | Crecimiento L2 | Reconstruct aleatorio: niveles (bid, ask) 4269/3634 → 7328/6467. Diffs añaden precios fuera del snapshot REST 1000. Documentado en fase 2; no cruzó el libro. |
| H8 | C | MENOR | Huecos recv | 324 huecos recv-time >200 ms; **0** event-time. Coherente con flushes Parquet (fase 3). |
| H9 | A | MENOR | Secretos | `.env` gitignored y **inexistente**. Hits = placeholders `.env.example` + `SecretStr` + tests `key-123`. Sin claves reales. |
| H10 | E | MENOR | Informe backtest | `run_ofi_mm_backtest.py` incrusta el R² **histórico** 0–1.4 % / 22–29 % en el markdown aunque se corra otro dataset. Cosmético. |

## Fase A — Higiene

### Pytest (default, sin `RUN_INTEGRATION`)

```
276 passed, 4 skipped in 3.18–3.35 s
platform darwin, Python 3.12.13, pytest-9.1.1
Required test coverage of 80.0% reached. Total coverage: 93.54%
```

Skipped (esperado): 4 tests `integration` (`RUN_INTEGRATION=1`). Baseline previo: ~276 passed / ~93 %. **Sin regresión.**

Re-ejecutado **después** de añadir `--rest-probe-every` (opt-in): sigue 276 passed.

### Cobertura por módulo (term-missing)

| Módulo | Cover | Notas |
| --- | ---: | --- |
| `ingestion/live.py` | **59 %** | Más bajo incluido. Código HTTP de honestidad en vivo. |
| `metrics/mlofi.py` | 83 % | |
| `ingestion/binance_futures.py` | 87 % | Ramas WS/429/gap poco ejercidas en unit. |
| `metrics/stream.py` | 89 % | |
| `backtest/conversion.py` | 97 % | |
| `ingestion/latency_audit.py` | 95 % | |
| `ingestion/sync.py` | 96 % | |
| `metrics/batch.py` | 96 % | |
| `storage/reconstruct.py` | 95 % | |
| resto medido | 100 % | Incluye stubs ClickHouse/QuestDB (`NotImplementedError`). |
| `backtest/ofi_mm.py` | **omit** | No entra en el 93.54 %. |
| `backtest/runner.py` | **omit** | No entra en el 93.54 %. |

**Ningún módulo medido a 0 % o <50 %.** No se ignoraron.

### mypy / ruff

- `uv run mypy`: **Success: no issues found in 83 source files** (strict, `python_version = 3.12`).
- `uv run ruff check src tests scripts`: **All checks passed.**
- No se hizo churn de formato. Único cambio de código: flags opt-in en `scripts/record_l2.py` (sidecar REST/reloj; default 0 = comportamiento previo).

### Secretos

| Patrón | Resultado |
| --- | --- |
| `.env` tracked | **No.** `.gitignore` línea 14: `.env`. `git ls-files` no conoce `.env`. El archivo **no existe** en el workspace. |
| `.env.example` | Placeholders vacíos (`BINANCE_API_KEY=`, etc.). OK. |
| `src/order_flow/utils/config.py` | Campos `SecretStr \| None`. OK. |
| `tests/unit/test_config.py` | Fakes `key-123` / `secret-456`; `str(SecretStr) == "**********"`. OK. |
| notebooks / docs | Sin `sk-`, sin `AKIA`, sin PEM. |
| `api_key` / `password` literales | Sin asignaciones reales. |

## Fase B — Ingestión, 15 minutos wall-clock

**Comando:** ver apéndice. **Supervivencia:** **sí** (exit 0, `error: null`, elapsed 915.989 s vs pedido 900 s). **Sin traceback.**

| Contador | Valor |
| --- | ---: |
| Snapshots REST (cola) | 1 |
| Snapshots periódicos LOB | 884 |
| Deltas | 8801 (9.79 / s) |
| Trades `@trade` | 30617 (34.06 / s) |
| Total eventos persistidos | 885 + 8801 + 30617 = **40303** |
| Gaps de secuencia (`pu != u`) | **0** |
| Resyncs | **0** |
| Reconnects WS | **0** |
| HTTP 429 | 0 (no apareció en log) |
| Duración event-time | **898.8 s** |
| Huecos event-time >200 ms | **0** |
| Huecos recv-time >200 ms | 324 |
| Dual sockets | false (combined `trade` + `depth@100ms`) |

**Resolución de gaps:** no hubo ninguno. El primer `resync_success` del log (15:11:32, `last_update_id=11458271575535`) es el snapshot REST **inicial**, no un gap.

### Latencia event→local

Convención: `offset = local − server`. Offset negativo = reloj local **atrás**. Cruda negativa **no** es edge.

**Reloj** (`GET /fapi/v1/time`, n=15, inicio/sondas/final, helpers de `latency_audit`):

| | ms |
| --- | ---: |
| mean offset | **−185.389** |
| min / max offset | −251.672 / +51.687 |
| mean RTT HTTP | 415.776 |
| max RTT | 633.375 |
| mean midpoint offset | −393.277 |

Incertidumbre residual ~RTT/2 ≈ **200 ms**.

**Distribución (Parquet completo, no el cap 10k de FeedStats):**

| Serie | n | p50 cruda | p90 cruda | p99 cruda | p50 adj | p90 adj | p99 adj | mean cruda |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| todas | 40303 | −223.5 | −67.1 | 153.1 | −38.1 | 118.3 | **338.5** | −186.0 |
| depth (deltas) | 8801 | −231.5 | −153.3 | 73.7 | −46.1 | 32.1 | **259.1** | −209.1 |
| trade | 30617 | −220.7 | −56.6 | 153.2 | −35.3 | 128.8 | 338.6 | −178.3 |
| FeedStats (cap 10k) | 10000 | −222.1 | — | **+63.3** | — | — | — | −196.7 |

Ajuste: `raw − mean_offset` con offset −185.389 ms.

**Vs auditoría de latencia previa** (10k depth consecutivos, 3 reconnects): depth p99 ajustada **782.5 ms**. Esta captura **0 reconnects** → p99 depth ajustada **259 ms**. Misma escala de **cientos de ms**, no µs de colo. No se presenta el p50 crudo negativo como ventaja.

## Fase C — Order book + storage

### Reconstruct a 5 timestamps aleatorios (seed 20260902)

`reconstruct_book` sobre Parquet. Los 5 con BBO válido, spread 1 tick (0.1), **no cruzados**, `is_synced=True`.

| # | UTC | lastUpdateId | niveles bid/ask | BBO |
| --- | --- | ---: | --- | --- |
| 1 | 20:14:16Z | 11458287951497 | 4269 / 3634 | 77317.0 / 77317.1 |
| 2 | 20:19:20Z | 11458323745705 | 5955 / 6014 | 77209.9 / 77210.0 |
| 3 | 20:20:19Z | 11458330257908 | 6296 / 5935 | 77252.7 / 77252.8 |
| 4 | 20:20:54Z | 11458334792756 | 6746 / 5963 | 77329.1 / 77329.2 |
| 5 | 20:23:48Z | 11458353296366 | 7328 / 6467 | 77392.5 / 77392.6 |

Replay **al final** vs libro en vivo: `ok=True`, mismo `last_update_id=11458372102529`, BBO 77384.1 / 77384.2. Mitad de captura: `ok=True`, no cruzado.

### Sidecar REST (`rest_probes.jsonl`, `--rest-probe-every 180`)

Comparación **reconstruct-at-lastUpdateId** (delta que *bracket* `U ≤ id ≤ u`) vs top-20 REST. Sin carrera contra un GET al final del universo.

| sonda | REST lastUpdateId | locate | match % (40 niv.) | mismatches | max \|Δqty\| | id_local − id_rest |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 0 | 11458271654405 | bracket_delta | **95.0** | 2 | 0.004 | +3780 |
| 1 | 11458289543385 | bracket_delta | **95.0** | 2 | 0.002 | +1828 |
| 2 | 11458311251941 | bracket_delta | **95.0** | 2 | 0.078 | +850 |
| 3 | 11458332344416 | bracket_delta | **97.5** | 1 | 0.001 | +6807 |
| 4 | 11458352324265 | bracket_delta | **97.5** | 1 | 0.001 | +3662 |
| 5 | 11458372655311 | nearest_prior (feed ya parado) | 65.0 | 14 | 0.779 | −552782 |
| 6 | 11458372906218 | nearest_prior (probe `end`) | 57.5 | 17 | 0.822 | −803689 |

**Durante la captura (0–4):** media **96.0 %** exactos. El 4 % restante es el residual ya descrito en fase 2/3: el libro local queda en `u` del diff 100 ms, el REST en un id interior al rango `[U,u]`; qty de 1–2 niveles se mueve. **Ningún libro cruzado.**

**Honestidad end-of-run** (carrera residual clásica, REST *después* del último delta): 38/40 match (95 %), REST `lastUpdateId` **por delante** (11458372154489 vs local 11458372102529), max \|Δqty\| 0.006. Igual de orden que fase 2 (39/40) y fase 3 (36/40).

No hay **divergencia de estado no cubierta por tests** que merezca CRÍTICO.

### DuckDB

`uv sync --extra analytics` (duckdb 1.5.5). Hive OK: `exchange=binance_futures/symbol=BTCUSDT/date=2026-09-02`.

```
snapshots  n=885    tmin=1788379892526000000  tmax=1788380791022000000  n_symbol=1  n_exchange=1
deltas     n=8801   tmin=1788379892552000000  tmax=1788380791328000000  n_symbol=1  n_exchange=1
trades     n=30617  tmin=1788379892762000000  tmax=1788380789403000000  n_symbol=1  n_exchange=1
```

Consulta **exit 0**. Particiones date/symbol confirmadas en disco.

## Fase D — Métricas (dataset independiente)

Scripts: `validate_ofi.py` / `validate_mlofi.py` sobre `data/qa-audit-15min`. Los markdown de referencia en `docs/math/` **no se sobrescribieron** (reports en el directorio de captura).

### OFI L1 R² — baseline 40 min vs esta auditoría 15 min

| τ | spec | R² 40 min (n) | R² 15 min (n) | β 40 min | β 15 min |
| --- | --- | ---: | ---: | ---: | ---: |
| 1s | lead-1 | 1.42 % (2376) | **3.92 %** (899) | 0.0491 | 0.0622 |
| 5s | lead-1 | 0.37 % (475) | **1.45 %** (180) | 0.0240 | 0.0354 |
| 10s | lead-1 | 0.07 % (237) | **6.06 %** (90) | **−0.0107** | **+0.0660** |
| 1s | contemp. | 22.04 % (2376) | **48.44 %** (899) | 0.1935 | 0.2186 |
| 5s | contemp. | 28.17 % (475) | **60.72 %** (180) | 0.2085 | 0.2287 |
| 10s | contemp. | 28.52 % (237) | **68.88 %** (90) | 0.2107 | 0.2224 |

Lead-1 1s 1.4 % → 3.9 %: ruido plausible de muestra. **Contemporáneo 22 % → 48 %** y **10 s lead-1 con cambio de signo** (t HAC 3.17 en n=90 vs t=−0.52 en n=237): **inestabilidad IMPORTANTE**, no bug de fórmula (H1). Cualitativamente sigue: contemporáneo ≫ lead-1; OFI no es predictor estable de la siguiente barra.

MLOFI no “arregla” lead-1 (1s lead-1 L1 3.92 % vs M5-sum 3.79 % vs M10-sum 3.77 %). Contemporáneo 10 s: L1 68.9 % → M5-sum **74.4 %** (Δ +5.5 pp vs +3–4 pp en la muestra larga). Misma dirección, magnitud muestra-dependiente.

### VPIN / NaN

| Check | Resultado |
| --- | --- |
| VPIN ∈ [0,1] | **sí** (único bucket emitido con V=vol/50=21.5158, window=50: **0.4868**) |
| VPIN NaN/Inf | **no** |
| OFI NaN/Inf | **no** (8801 eventos) |
| MLOFI Inf | **no**; padding NaN de niveles faltantes permitido; L1 finito en todos |
| CVD terminal | 85.4900 (30617 trades) |

VPIN n=1 es el diseño (hace falta un window de 50 buckets; 15 min de volumen solo llena ~50 buckets). No es un fallo.

### Streaming vs batch (mismo input 15 min)

| Métrica | match |
| --- | --- |
| OFI `OfiAccumulator` vs `compute_ofi_events` vs `ofi_events_from_capture` | **pass** (8801) |
| MLOFI M=10 | **pass** (8801) |
| CVD | **pass** (30617) |
| VPIN aggressor | **pass** |

Ya existían tests unitarios (`test_ofi_stream.py`, `test_metrics_batch.py`). Esta corrida lo confirma **sobre el tape real**. Si hubieran divergido sería CRÍTICO; **no divergieron**. No se tocó el núcleo.

## Fase E — Backtest nautilus 1.231.0

`uv sync --extra backtest`. `scripts/run_ofi_mm_backtest.py` maker post-only **y** crossing.

### Conservación adapter

| | Parquet | Documentado | Nautilus | Δ |
| --- | ---: | --- | ---: | ---: |
| Snapshots | 885 | omitir periódicos si mismo id | 885 CLEAR+ADD | skip **0** (H2) |
| Deltas | 8801 | — | 8801 batches | 0 |
| **Book batches** | 885+8801=9686 | — | **9686** | **0** |
| Trades | 30617 | qty==0 drop | | |
| qty==0 | 181 | sí | | |
| **TradeTick** | 30436 | | **30436** | **0** |

Runner log: `loaded 9686 book batches and 30436 trades`. **Sin pérdida ni duplicación inexplicada** una vez se cuenta que el skip periódico **no ocurre**. H2 documenta el desajuste docstring vs realidad; no se “arregló”.

### PnL / fills vs corrida original ~39.6 min

| | 39.6 min (baseline) | 15.0 min (auditoría) | por minuto 40 min | por minuto 15 min |
| --- | ---: | ---: | ---: | ---: |
| Maker PnL USDT | −71.935 | **−8.260** | −1.82 | **−0.55** |
| Maker fill rate | 28.77 % | **26.38 %** | | |
| Maker fills | 1874 maker / 0 taker | **345 / 0** | | |
| Crossing PnL | −1335.302 | **−599.973** | −33.7 | **−40.0** |
| Crossing fees | 1332.619 | **599.048** | | |
| Crossing fill | 100 % | **100 %** | | |
| Crossing taker fills | 43238 | **19372** | | |

Misma **cualitativa**: maker pérdida pequeña, 0 fills taker; crossing destruye PnL **casi entero en fees taker** (599.05 / 599.97 ≈ 99.8 %). Fill rate maker ~26–29 %. **No** hay reversión (crossing rentable / maker gran win). Escala 15 vs 40 min no es lineal (régimen + menos órdenes: 1308 vs 6513 submits maker).

## Qué NO se corrigió y por qué

Los CRÍTICO de integridad **no aparecieron**. Los IMPORTANTE se **documentan y se dejan**:

1. **H1 R² inestable** — tocar OFI/MLOFI invalidaría comparaciones con `docs/math/*`. La evidencia apunta a muestra, no a `e_n`.
2. **H2 skip de snapshots periódicos en el adapter** — cambiar el sort/skip **cambia el tape nautilus** (CLEAR+ADD 1 Hz). Las capas de backtest dependen de eso. Qty absoluta sugiere idempotencia; hace falta un test de igualdad de libro *después* de skip vs no-skip, no un parche opaco.
3. **H3 cap 10k de latencia** — ampliar el buffer cambia lo que escribe `capture_meta` y los informes que lo leen (`validate_ofi` usó p99 **+63 ms** del meta, no el p99 Parquet).
4. **H4 mismatches REST 4 %** — residual de protocolo 100 ms; “arreglarlo” fingiendo match exacto mentiría.

Instrumentación permitida: `--rest-probe-every` / `--rest-probe-levels` en `scripts/record_l2.py` (default **0**, Parquet idéntico al default anterior). Script observacional `data/qa-audit-15min/observe_cde.py` (no es producto).

**No** se colocaron órdenes. **No** git commit. **No** remotos.

## Apéndice: comandos exactos y rutas

```bash
# A — higiene
uv run pytest
uv run mypy
uv run ruff check src tests scripts

# B — 15 min (sidecar REST cada 180 s)
uv run python scripts/record_l2.py --symbol BTCUSDT --seconds 900 \
  --out "data/qa-audit-15min" --snapshot-interval 1 \
  --rest-probe-every 180 --rest-probe-levels 20 --log-level INFO

# C — DuckDB
uv sync --extra analytics
uv run python -c "import duckdb; ..."  # counts hive snapshots/deltas/trades

# D — métricas (reports en data/, no pisan docs/math)
uv sync --extra notebooks
uv run python scripts/validate_ofi.py \
  --root "data/qa-audit-15min" --report "data/qa-audit-15min/ofi_validation.md" --json
uv run python scripts/validate_mlofi.py \
  --root "data/qa-audit-15min" --report "data/qa-audit-15min/mlofi_validation.md" --json

# E — backtest
uv sync --extra backtest
uv run python scripts/run_ofi_mm_backtest.py \
  --root "data/qa-audit-15min" --report "data/qa-audit-15min/ofi_mm_results.md"
```

**Rutas**

| Qué | Path |
| --- | --- |
| Captura Parquet | `data/qa-audit-15min/{snapshots,deltas,trades}/exchange=binance_futures/symbol=BTCUSDT/date=2026-09-02/` |
| Meta | `data/qa-audit-15min/capture_meta.json` |
| Sidecar REST | `data/qa-audit-15min/rest_probes.jsonl` (7 líneas) |
| Reloj | `data/qa-audit-15min/clock_offsets.jsonl` (15 líneas) |
| Observación C–E | `data/qa-audit-15min/observe_cde.json` |
| Este informe | `docs/qa_audit_report.md` |
