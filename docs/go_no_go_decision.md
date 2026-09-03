# Decisión GO / NO-GO — capa de ejecución en vivo

**Fecha:** 2026-09-02 (UTC 19:40:59Z, fin de sonda ~20:01Z).
**Máquina:** `Carloair.local`. Red de investigación doméstica. **No** es colocación, **no** es un VPS en AWS Tokyo, **no** es un cross-connect.
**Veredicto:** **NO-GO** para capa de ejecución en vivo, **incluido testnet**. **NO-GO** para capital. El sistema vale como **plataforma de investigación**; no como estrategia de ejecución rentable con esta infraestructura y esta evidencia.

Cifras de latencia de **esta** corrida: [latency/latency_audit_results.md](latency/latency_audit_results.md).
OFI empírico: [math/ofi_validation.md](math/ofi_validation.md).
MLOFI: [math/mlofi_validation.md](math/mlofi_validation.md).
Backtest nautilus 1.231.0: [backtest/ofi_mm_results.md](backtest/ofi_mm_results.md).

No hay capa de enrutado de órdenes en el repo. Esta nota es el candado **antes** de escribirla.

---

## 1. ¿Latencia para cruzar el spread (take agresivo) o solo MM pasivo?

**Solo MM pasivo, y ni siquiera como asignación de capital.** El take agresivo con señal OFI queda fuera.

OFI L1 Cont, ~40 min Binance USD-M BTCUSDT, misma captura del proyecto:

| τ | R² lead-1 (predictivo) | R² contemporáneo |
| --- | ---: | ---: |
| 1 s | 1.4 % | ~22 % |
| 5 s | 0.4 % | ~28 % |
| 10 s | ~0 % | ~29 % |

Lead-1 es lo que necesitas para **levantar** liquidez *antes* de que el movimiento ya esté en el libro y en los prints. Con R² predictivo ≈ 0, **latencia cero no crea edge de take**. El take llegaría a un libro que ya se movió; estarías pagando el spread y el taker fee sobre información contemporánea, no adelantada.

MLOFI no arregla lead-1; solo suma 3–4 pp al R² contemporáneo a 10 s. VPIN es toxicidad retrospectiva, no alerta temprana (Andersen–Bondarenko 2014).

La latencia, entonces, solo dice **cuán rancio** es un OFI contemporáneo frente a un competidor colocated. Hay que mirar **p99 / p99.9, no la media**.

Sonda pública, 10 000 mensajes consecutivos `@depth@100ms` + 36 092 `@trade` (BTCUSDT, mainnet, sin API key, sin órdenes):

| Serie | n | min | p50 | p90 | p99 | p99.9 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| depth cruda (ms) | 10000 | −358.1 | −305.4 | −112.5 | 585.9 | 3750.5 | 4647.0 | −234.3 |
| depth ajustada (ms) | 10000 | −161.4 | −108.7 | 84.2 | **782.5** | **3947.2** | 4843.6 | −37.6 |
| trade cruda (ms) | 36092 | −357.7 | −280.3 | 35.0 | 3387.8 | 4430.2 | 4542.0 | −122.7 |
| trade ajustada (ms) | 36092 | −161.0 | −83.6 | 231.7 | 3584.5 | 4626.8 | 4738.7 | 74.0 |

Offset de reloj (`GET /fapi/v1/time`, `offset = local − server`, n=15, inicio/mitad/final): media **−196.7 ms** (reloj local detrás). RTT HTTP medio **395.3 ms**, máx **932.4 ms**. Incertidumbre residual del offset ~RTT/2 ≈ **200 ms**, más NTP y asimetría. El p50 ajustado negativo **no** es “más rápido que colo”: es error de reloj + HTTP lento. La corrida de honestidad de 60 s ya había dado media cruda **−34 ms** por la misma razón.

Inter-llegada de `E` en depth: p50 = p99 = **102 ms**. Binance ya discretiza el libro a **100 ms**. Ese throttle **domina** cualquier last-mile de unos pocos milisegundos. Huecos locales >250 ms: **589** (p99 local 421 ms; máx 4.7 s, alineado con 3 timeouts de handshake). El exchange emite a 100 ms; este proceso no siempre consume a 100 ms.

Comparación institucional (recuperado 2026-09-02; marketing marcado como tal):

| Fuente | Orden de magnitud | Qué es |
| --- | --- | --- |
| Databento ([live](https://databento.com/live)) | p90 **42 µs** cross-connect / **590 µs** internet | Marketing cuantitativo de vendor, no Binance |
| Databento [dedicated](https://databento.com/docs/architecture/dedicated-connectivity-guide) | p90 **42.4 µs** colo; internet **0.5+ ms** | Marketing, números explícitos |
| Nanoconda [CME MDP3 vs iLink](https://nanoconda.com/blog/cme-trade-summary-vs-private-fills/) | MD mediana **266 µs**; MSGW **203 µs** | Empírico colocated, timestamps CME |
| Rithmic [API suite](https://www.rithmic.com/products/api-suite) | Diamond **<250 µs** tick-to-trade colo; API+ **<1 ms** | Marketing de vendor; no es Binance |
| CQG [Client APIs](https://www.cqg.com/products/cqg-apis/client-apis) | “one millisecond for data round-trip” | Folleto; **no** hay spec pública en µs |
| Binance Diff. Depth (oficial) | update **100 / 250 / 500 ms** | El path de OFI ya va a 100 ms |
| Jane Street [magic-trace](https://blog.janestreet.com/magic-trace/) | respuesta **≪ 250 µs** | Orden de magnitud de ingeniería, no SLA |
| Esta máquina | depth p99 ajustado **783 ms**; p99.9 **3.9 s** | Hogar / investigación |

p99 depth ajustado (~0.8 s) frente a colo (decenas–cientos de **µs**) es **tres órdenes de magnitud**. Aunque el ajuste tenga ±200 ms de error, no sales de la escala de **cientos de milisegundos**. Un hop colocated de market data es ~1000× más corto. El throttle de 100 ms del depth oficial ya mata el take sobre un libro que el colo ve a granularidad de µs.

Backtest nautilus 1.231.0, misma captura: maker post-only **−72 USDT** (1874 fills maker, 0 taker, fill 28.8 %); crossing **−1335 USDT**, casi todo taker fee. Cruzar **destruyó** PnL por comisiones, no porque “OFI fallara”.

**Respuesta explícita:** esta latencia **no** permite cruzar el spread de forma agresiva con señal OFI. Como mucho, MM pasivo de baja frecuencia (proveer liquidez con skew), y eso como experimento, no como edge.

---

## 2. ¿Qué es viable: MM pasivo de baja frecuencia, o solo investigación sin edge de ejecución?

Combinado:

1. OFI **no es predictivo** en estos datos (lead-1 R² 1.4 % / 0.4 % / ~0 %).
2. La información que sí explica Δmid es **contemporánea** (R² ~22–29 %). Llega cuando el movimiento ya está en la ventana.
3. El libro oficial ya viene a **100 ms**.
4. Last-mile medido: p99 ajustado **783 ms**, p99.9 **3.9 s**, 3 reconnects por handshake, 589 huecos locales >250 ms.
5. Crossing en backtest mata el PnL por **fees taker**.

**Viable:** investigación académica / ingeniería de captura, métricas y replay. El pipeline (WS público → libro → OFI/MLOFI/VPIN/CVD → Parquet → nautilus / hftbacktest) está cableado y es honesto sobre lo que no es.

**No viable:** take agresivo. **No viable:** MM pasivo como estrategia de ejecución con capital. Un MM pasivo de baja frecuencia se puede **seguir estudiando** en replay (sin latencia de red, ver [backtest_limitations.md](backtest_limitations.md)); eso no autoriza testnet ni mainnet.

Caveat, no reversión: 40 min / un símbolo / un venue cripto no es un paper de un mes de TAQ. Más datos no van a convertir R² lead-1 ≈ 0 y p99 de cientos de ms en un edge de take sobre este last-mile. Si algún día hay colo + feed no throttled a 100 ms + R² predictivo medido, se reabre el expediente. Hoy no.

---

## 3. Fill rate corregido por cola (hftbacktest 2.4.4)

Misma captura de la auditoría QA (`data/qa-audit-15min`, ~15 min, BTCUSDT). Misma economía OFI-MM (0.001 BTC, mid ± 2 ticks, post-only). Tabla completa: [backtest/queue_position_comparison.md](backtest/queue_position_comparison.md). Algoritmo: [backtest/hftbacktest_queue.md](backtest/hftbacktest_queue.md).

| | nautilus 1.231 (`queue_position` FIFO + clip) | hftbacktest `ProbQueueModel` + `PowerProbQueueFunc(n=2)` |
| --- | ---: | ---: |
| Maker fill rate | 26.38 % | **27.72 %** (Δ **+1.34 pp**) |
| Maker fills | 345 / 0 taker | 435 / 0 taker |
| Maker PnL USDT | −8.26 | **−12.09** |
| Crossing PnL | −600 (100 % fill, fees 599) | −447 (99.99 % fill, fees 447) |

nautilus 1.231 **sí** modelaba cola (FIFO del size mostrado). hftbacktest usa el modelo probabilístico HFT (bajas de qty partidas delante/detrás con \(f(x)=x^2\)). No es “sin cola vs con cola”.

**Esto no cambia la conclusión. La refuerza** porque:

1. El fill rate no se cae a un régimen donde nautilus hubiera *inventado* fills. La banda 26–28 % aguanta bajo el modelo más específico de HFT. nautilus **no** estaba siendo optimista frente a ProbQueue (si acaso un poco más conservador).
2. El número que debe pesar más que el prompt 5 es el PnL maker **con** ProbQueue: **−12.09 USDT** (peor que −8.26). Más fills no son un edge: son más fees y peor inventario marcado.
3. Crossing sigue siendo un agujero de taker fee. La cola no es el mecanismo del take.
4. Siguen en pie R² lead-1 débil y last-mile de cientos de ms.

**Sigue sin haber capa de ejecución en vivo** (tampoco testnet). Este prompt solo añade un segundo adapter de replay.

---

## 4. GO / NO-GO

| Pregunta | Decisión |
| --- | --- |
| ¿Capa de ejecución en vivo (mainnet)? | **NO-GO** |
| ¿Capa de ejecución en **testnet**? | **NO-GO** (el candado es anterior a cualquier orden) |
| ¿Take agresivo con OFI? | **NO-GO** (R² lead-1 ≈ 0; latencia irrelevante para crear predicción) |
| ¿MM pasivo con capital? | **NO-GO** |
| ¿Seguir midiendo / capturando / backtesteando sin órdenes? | **GO** como investigación |
| ¿Este stack es un edge de ejecución? | **No** |

**Frase única:** **NO-GO para la capa de ejecución en vivo, incluido testnet; plataforma de investigación, no estrategia rentable con esta evidencia y esta red.**

---

## Cómo repetir la sonda (market data pública, sin órdenes)

```bash
uv run python scripts/latency_audit.py --symbol BTCUSDT --n-events 10000
```
