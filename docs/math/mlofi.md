# MLOFI — Multi-Level Order Flow Imbalance

Implementación: `order_flow.metrics.mlofi` (núcleo numpy),
`order_flow.metrics.stream.MlofiAccumulator` (streaming desde
`OrderBook.to_arrays` / `depth`) y
`order_flow.metrics.batch.mlofi_events_from_capture` (replay Parquet
snapshots+deltas → el mismo \(e^m_n\)).

Recuperación de fuentes: **2026-09-02**.

## Fuente primaria

Xu, K., Gould, M. D. & Howison, S. D. (2019). *Multi-Level Order-Flow
Imbalance in a Limit Order Book*. arXiv:1907.06230 (revisado octubre 2019).
Versión de revista: *Market Microstructure and Liquidity*, 4(03n04), 1950011
(DOI [10.1142/S2382626619500114](https://doi.org/10.1142/S2382626619500114)).

- HTML: [https://arxiv.org/html/1907.06230v2](https://arxiv.org/html/1907.06230v2)
- PDF: [https://arxiv.org/pdf/1907.06230](https://arxiv.org/pdf/1907.06230)

El trabajo es una extensión constructiva de Cont, Kukanov & Stoikov (2014):
la misma contribución \(e_n\) se evalúa en el \(m\)-ésimo mejor bid/ask,
\(m = 1,\ldots,M\). El resultado es un **vector** de dimensión \(M\), no un
escalar. Cuando \(M = 1\), MLOFI **es** OFI.

## Fórmula (Xu et al. §3.1, eqs. 9–12)

Entre dos eventos consecutivos del libro \(\tau_{n-1} \to \tau_n\), en el
nivel \(m\) (1 = best):

\[
\Delta W^m(\tau_n) =
\begin{cases}
r^m(\tau_n) & \text{si } b^m(\tau_n) > b^m(\tau_{n-1}) \\
r^m(\tau_n) - r^m(\tau_{n-1}) & \text{si } b^m(\tau_n) = b^m(\tau_{n-1}) \\
- r^m(\tau_{n-1}) & \text{si } b^m(\tau_n) < b^m(\tau_{n-1})
\end{cases}
\]

\[
\Delta V^m(\tau_n) =
\begin{cases}
- q^m(\tau_{n-1}) & \text{si } a^m(\tau_n) > a^m(\tau_{n-1}) \\
q^m(\tau_n) - q^m(\tau_{n-1}) & \text{si } a^m(\tau_n) = a^m(\tau_{n-1}) \\
q^m(\tau_n) & \text{si } a^m(\tau_n) < a^m(\tau_{n-1})
\end{cases}
\]

\[
e^m_n = \Delta W^m(\tau_n) - \Delta V^m(\tau_n)
\]

Equivalente a la forma con indicadores de Cont et al. que ya usa
`ofi_contributions` (funciona fila a fila con trailing shape `(M,)`):

\[
\begin{aligned}
e^m_n
&= \mathbb{1}\{P^{b,m}_n \ge P^{b,m}_{n-1}\} q^{b,m}_n
 - \mathbb{1}\{P^{b,m}_n \le P^{b,m}_{n-1}\} q^{b,m}_{n-1} \\
&\quad - \mathbb{1}\{P^{a,m}_n \le P^{a,m}_{n-1}\} q^{a,m}_n
 + \mathbb{1}\{P^{a,m}_n \ge P^{a,m}_{n-1}\} q^{a,m}_{n-1}
\end{aligned}
\]

Nivel \(m=1\) (columna 0 del array) **reproduce OFI exactamente**. Los tests
lo exigen. En una ventana de tiempo \(k\),

\[
\mathrm{MLOFI}^m_k = \sum_{\{n : t_{k-1} < \tau_n \le t_k\}} e^m_n
\]

igual que \(\mathrm{OFI}_k\) en Cont et al. eq. (5).

## Lo que Xu et al. **sí** afirman

Sobre 6 acciones líquidas de Nasdaq en 2016, ajustan una relación **lineal
contemporánea** \(\Delta P_k \sim \mathrm{MLOFI}_k\) (el mismo alineamiento
que Cont et al., *no* lead-1). Con OLS multivariante hay multicolinealidad
fuerte entre niveles vecinos; por eso usan Ridge. Con Ridge, el RMSE
*out-of-sample* baja al incluir más niveles: del orden de **65–75 %** en
acciones *large-tick* y **15–30 %** en *small-tick* al pasar de \(M=1\) a
\(M=10\), relativo al OFI de un nivel. El \(R^2\) ajustado in-sample también
sube con \(M\).

Eso es un resultado empírico en **acciones US, contemporáneo, con Ridge**.
No es un teorema, no es cripto, no es predictivo (lead-1).

## Lo que **no** encontramos

Búsqueda (2026-09-02) de un paper que afirme que MLOFI **“irrefutablemente”**
gana al OFI L1 en \(R^2\): **no encontrado**. Ninguna fuente verificable usa
esa formulación. Cont et al. (2014), apéndice B3, al contrario, reportaron
que añadir 5 niveles con OLS **apenas** mejoraba el ajuste y concluyeron que
el flujo más allá del BBO influye poco. Xu et al. discrepan *en su muestra y
con Ridge*, no “irrefutablemente”.

Un working paper posterior (Mertens et al. 2019, citado por Xu §2.2.5)
modela el impacto como variable latente sobre OFI L1; es otra extensión, no
una prueba de superioridad de MLOFI.

**Postura de este sistema:** MLOFI es una extensión razonable *por
construcción* (el mismo \(e_n\) en cada cola). Si extra profundidad mejora
el \(R^2\) en los perps de Carlos es una pregunta empírica que responde
`scripts/validate_mlofi.py`, no el abstract de Xu. No se mezcla VPIN en esa
regresión.

## Pesos entre niveles (conveniencia nuestra, no del paper)

Xu et al. **no** suman los niveles con un vector de pesos: regresan el
vector. Para comparar peras con peras en un OLS *univariante* (lo que pidió
Carlos: “OFI de un nivel vs MLOFI de 5 vs MLOFI de 10”) hace falta un
escalar. Definimos

\[
S^{(M)}_n = \sum_{m=1}^{M} w_m e^m_n
\]

**Default: pesos iguales** \(w_m = 1\) (suma llana). No es 1/m, no viene del
paper. Si se quieren pesos \(1/m\), pasar
`level_weights(M, scheme="inverse")`. La alternativa honesta de “más
parámetros” es el OLS **multivariante** con \(M\) regresores: el \(R^2\)
in-sample **sube por construcción** al añadir columnas; hay que decirlo en
el informe.

## Niveles ausentes, unsynced, forma

- Entrada: arrays `(N, M)`; columna 0 = best. Un 1-D se trata como `(N, 1)`.
- Salida de eventos: `(N-1, M)`.
- Si el libro tiene menos de \(M\) niveles, `OrderBook.to_arrays(M)` **rellena
  con NaN** (no se trunca el eje). Un tramo con precio NaN produce \(e^m_n =
  \mathrm{NaN}\) en esa columna (`ofi_contributions`).
- Unsynced / BBO vacío: el acumulador streaming se resetea (igual que OFI);
  el replay batch abre época nueva tras resync y no cruza \(e_n\) entre épocas.
- \(M\) configurable; default de los adaptadores streaming/batch: **5**.
  Se soporta **10** (la captura de Carlos guarda el libro REST completo,
  ~1000 niveles/lado; no hace falta recapture).

## Relación con OFI L1

`compute_mlofi_events(...)[:, 0]` ≡ `compute_ofi_events(...)`.
`MLOFI^1_k` en una ventana de tiempo ≡ `OFI_k` de `compute_ofi_time_windows`.
Si eso se rompe, es un bug, no una “mejora”.

## API

- `compute_mlofi_events` → `(N-1, M)`
- `aggregate_mlofi_levels` → suma ponderada (default: unos)
- `level_weights(M, scheme="equal"|"inverse")`
- `compute_mlofi` → ventanas de *eventos* (no de tiempo)
- `compute_mlofi_time_windows` → misma rejilla 1s/5s/10s que OFI
- Streaming: `MlofiAccumulator(levels=5)`
- Batch: `mlofi_events_from_capture(..., levels=10)` vía
  `iter_lm_ticks` / `OrderBook.to_arrays`

OLS empírica (misma captura que fase 4): [mlofi_validation.md](mlofi_validation.md).

## Referencias

1. Xu, K., Gould, M. D. & Howison, S. D. (2019). Multi-Level Order-Flow
   Imbalance in a Limit Order Book. arXiv:1907.06230.
2. Cont, R., Kukanov, A. & Stoikov, S. (2014). The Price Impact of Order
   Book Events. *Journal of Financial Econometrics* 12(1), 47–88.
   (OFI L1; apéndice B3: cinco niveles, mejora leve con OLS.)
