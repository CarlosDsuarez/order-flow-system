# Validación empírica de MLOFI (Xu-Gould-Howison 2019) vs OFI L1

Generado: **2026-09-02 17:07:01Z** en `Carloair.local`.
Script: `scripts/validate_mlofi.py` (fuente de verdad; el notebook solo lo documenta).

## Setup

- Exchange: `binance_futures` (Binance USD-M, mainnet `fapi` / `fstream`)
- Símbolo: `BTCUSDT`
- Directorio: `/Users/carlos_suarez7/ORDER FLOW/data/live-btcusdt-45min`
- Duración de la serie L1: **2375.7 s** (meta elapsed `2376.2 s`, pedida `2700.0 s`)
- Estados sincronizados: 19667; eventos $e^m_n$: 19664; niveles pedidos: 10; épocas: 3
- Updates / s: 8.28; deltas/s (storage): 8.28
- Latencia observada (recv - event): media 0.0593 s, p99 0.1816 s
- Gaps / resyncs / reconnects (meta): None / 3 / None
- Dual sockets: False
- Trades persistidos: 146394 (no entran en esta regresión)
- Recapture: **no**. Los snapshots guardan el libro REST completo (>> 10 niveles). Misma captura que la fase 4.

Tick de BTCUSDT USD-M: **0.1 USD**. Δmid en **precio** (USD), no en ticks.

## Especificación

Misma rejilla y alineamiento que `scripts/validate_ofi.py` / [ofi_validation.md](ofi_validation.md).
Ventana \(k\): \([t_0 + k\tau,\ t_0 + (k+1)\tau)\), \(\tau \in \{1,5,10\}\) s.
Nivel \(m\): la misma \(e_n\) de Cont et al. en el \(m\)-ésimo best (Xu et al. §3.1). Columna 0 ≡ OFI L1.
\(\mathrm{MLOFI}^m_k = \sum e^m_n\) en la ventana.
Mid: último mid **L1** de la barra (carry-forward). Barras con resync se tiran.

**Lead-1 (lo que pidió Carlos, predictivo):**

\[\Delta \mathrm{mid}_{k+1} = \alpha + \beta\, X_k + \varepsilon_{k+1}\]

**Contemporánea (lo que estima Cont et al. y Xu et al.):**

\[\Delta \mathrm{mid}_k = \alpha + \beta\, X_k + \varepsilon_k\]

Features univariantes (comparación justa, un solo β):

- **L1 OFI**: $X_k = \mathrm{MLOFI}^1_k$ (≡ OFI)
- **M5-sum**: $X_k = \sum_{m=1}^{5} e^m_k$ (pesos iguales; **no** viene de Xu)
- **M10-sum**: $X_k = \sum_{m=1}^{10} e^m_k$

Features multivariantes (extra, **R² in-sample sube al añadir columnas**):

- **M5-multi** / **M10-multi**: $X_k$ es el vector de 5 (resp. 10) niveles.

OLS con errores HAC Newey-West, lags \(= \lfloor 4(n/100)^{2/9}\rfloor\).
**VPIN no se mezcla** en esta regresión.

## Resultados univariantes (L1 vs suma M=5 vs suma M=10)

| τ | spec | feature | n | β | EE HAC | t | R² | lags NW |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1s | lead-1 | L1 OFI | 2376 | 0.0491 | 0.0070 | 7.0044 | 0.0142 | 8 |
| 1s | lead-1 | M5-sum | 2376 | 0.0383 | 0.0053 | 7.2185 | 0.0146 | 8 |
| 1s | lead-1 | M10-sum | 2376 | 0.0325 | 0.0045 | 7.1969 | 0.0155 | 8 |
| 5s | lead-1 | L1 OFI | 475 | 0.0240 | 0.0134 | 1.7892 | 0.0037 | 5 |
| 5s | lead-1 | M5-sum | 475 | 0.0184 | 0.0098 | 1.8764 | 0.0038 | 5 |
| 5s | lead-1 | M10-sum | 475 | 0.0141 | 0.0072 | 1.9468 | 0.0036 | 5 |
| 10s | lead-1 | L1 OFI | 237 | -0.0107 | 0.0204 | -0.5247 | 0.0007 | 4 |
| 10s | lead-1 | M5-sum | 237 | -0.0094 | 0.0154 | -0.6126 | 0.0010 | 4 |
| 10s | lead-1 | M10-sum | 237 | -0.0090 | 0.0121 | -0.7472 | 0.0014 | 4 |
| 1s | contemp. | L1 OFI | 2376 | 0.1935 | 0.0068 | 28.2926 | 0.2204 | 8 |
| 1s | contemp. | M5-sum | 2376 | 0.1625 | 0.0062 | 26.4206 | 0.2630 | 8 |
| 1s | contemp. | M10-sum | 2376 | 0.1338 | 0.0064 | 20.9814 | 0.2626 | 8 |
| 5s | contemp. | L1 OFI | 475 | 0.2085 | 0.0124 | 16.8351 | 0.2817 | 5 |
| 5s | contemp. | M5-sum | 475 | 0.1678 | 0.0097 | 17.2787 | 0.3197 | 5 |
| 5s | contemp. | M10-sum | 475 | 0.1335 | 0.0100 | 13.3051 | 0.3212 | 5 |
| 10s | contemp. | L1 OFI | 237 | 0.2107 | 0.0138 | 15.3224 | 0.2852 | 4 |
| 10s | contemp. | M5-sum | 237 | 0.1703 | 0.0102 | 16.7352 | 0.3167 | 4 |
| 10s | contemp. | M10-sum | 237 | 0.1381 | 0.0091 | 15.1860 | 0.3238 | 4 |

## Resultados multivariantes (aviso: más parámetros ⇒ más R² in-sample)

| τ | spec | feature | n | k | R² | lags NW |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 1s | lead-1 | M5-multi | 2376 | 5 | 0.0176 | 8 |
| 1s | lead-1 | M10-multi | 2376 | 10 | 0.0214 | 8 |
| 5s | lead-1 | M5-multi | 475 | 5 | 0.0077 | 5 |
| 5s | lead-1 | M10-multi | 475 | 10 | 0.0159 | 5 |
| 10s | lead-1 | M5-multi | 237 | 5 | 0.0123 | 4 |
| 10s | lead-1 | M10-multi | 237 | 10 | 0.0163 | 4 |
| 1s | contemp. | M5-multi | 2376 | 5 | 0.2657 | 8 |
| 1s | contemp. | M10-multi | 2376 | 10 | 0.2723 | 8 |
| 5s | contemp. | M5-multi | 475 | 5 | 0.3239 | 5 |
| 5s | contemp. | M10-multi | 475 | 10 | 0.3354 | 5 |
| 10s | contemp. | M5-multi | 237 | 5 | 0.3253 | 4 |
| 10s | contemp. | M10-multi | 237 | 10 | 0.3407 | 4 |

## Literatura vs estos datos

Xu, Gould & Howison (2019), *Market Microstructure and Liquidity* / arXiv:1907.06230: en 6 acciones Nasdaq 2016, Ridge **contemporáneo** mejora el RMSE OOS al pasar de $M=1$ a $M=10$. **No** es lead-1, **no** es cripto, **no** es OLS univariante con suma de niveles.

Búsqueda (2026-09-02) de un paper que afirme que MLOFI gana a OFI L1 **«irrefutablemente»** en R²: **no encontrado**. Cont et al. apéndice B3 vieron solo una mejora leve con 5 niveles y OLS.

## Conclusión

Lead-1 sigue ~0 al añadir profundidad: **MLOFI-5/10 no convierten OFI en un predictor** de la siguiente ventana en esta muestra. La suma M=5/10 **sí mueve** el R² contemporáneo univariante (Δ ≈ 0.0386 vs L1 a 10 s). Sigue siendo un símbolo, ~40 min, cripto: no transfiere el paper de Xu (Nasdaq, Ridge, OOS). No se encontró un paper que afirme que MLOFI gana a OFI L1 «irrefutablemente» en R²; Xu et al. (2019) reportan RMSE Ridge OOS en acciones US contemporáneas, otro venue y otro estimador. VPIN no entra en esta regresión.

## Caveats

- Misma captura ~40 min / un símbolo (BTCUSDT) que la fase 4.
- Suma de niveles es conveniencia nuestra; Xu regresa el vector (Ridge).
- R² multivariante in-sample no es evidencia out-of-sample.
- Libro L2 agregado 100 ms, no el L3 de Xu.
- No se asume que los resultados de Xu se transfieran a perps cripto.

## Reproducción

```bash
uv run --extra notebooks python scripts/validate_mlofi.py \
  --root /Users/carlos_suarez7/ORDER FLOW/data/live-btcusdt-45min --report docs/math/mlofi_validation.md
```
