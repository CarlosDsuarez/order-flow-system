# OFI — Order Flow Imbalance (solo L1)

Implementación: `order_flow.metrics.ofi` (núcleo numpy),
`order_flow.metrics.stream.OfiAccumulator` (streaming) y
`order_flow.metrics.batch` (Parquet → mismos arrays).

**Esta fase implementa únicamente OFI de un nivel (best bid/ask).** MLOFI
(Xu, Gould & Howison, 2019) vive en `mlofi.py` y no se mezcla aquí.

## Fuente primaria

Cont, R., Kukanov, A. & Stoikov, S. (2014). *The Price Impact of Order Book
Events*. Journal of Financial Econometrics, 12(1), 47–88.

- Preprint arXiv: [https://arxiv.org/abs/1011.6402](https://arxiv.org/abs/1011.6402)
- PDF: [https://arxiv.org/pdf/1011.6402](https://arxiv.org/pdf/1011.6402)
- Recuperado el **2026-09-02**. Las fórmulas de abajo se transcribieron del
  preprint arXiv (versión de marzo 2011). En ese PDF la contribución \(e_n\)
  aparece como fórmula exhibida **sin número** en la sección 2.1; el modelo
  estadístico es la eq. (2) y la OLS empírica la eq. (4).

## Notación (sección 2.1)

El paper se restringe al *Level I order book*: órdenes límite en el mejor bid
y el mejor ask. Cada observación \(n\) es la cuádrupla

\[
(P^B_n,\ q^B_n,\ P^A_n,\ q^A_n)
\]

- \(P^B_n\): precio del mejor bid (demanda)
- \(q^B_n\): tamaño de la cola en el mejor bid (acciones / contratos)
- \(P^A_n\): precio del mejor ask (oferta)
- \(q^A_n\): tamaño de la cola en el mejor ask

Se comparan dos observaciones consecutivas
\((P^B_{n-1}, q^B_{n-1}, P^A_{n-1}, q^A_{n-1})\) y
\((P^B_n, q^B_n, P^A_n, q^A_n)\). Entre ellas, según el paper, solo puede
ocurrir uno de: subida/bajada de demanda (precio o tamaño del bid) o
subida/bajada de oferta (precio o tamaño del ask).

## Definición de \(e_n\) (sección 2.1, fórmula exhibida)

Cita parafraseada del paper: \(e_n\) «measures the contribution of the
\(n\)-th event to the size of bid and ask queues». Fórmula exacta:

\[
e_n
=
\mathbb{1}_{\{P^B_n \ge P^B_{n-1}\}} q^B_n
-
\mathbb{1}_{\{P^B_n \le P^B_{n-1}\}} q^B_{n-1}
-
\mathbb{1}_{\{P^A_n \le P^A_{n-1}\}} q^A_n
+
\mathbb{1}_{\{P^A_n \ge P^A_{n-1}\}} q^A_{n-1}
\]

Casuística que el paper hace explícita (mismo párrafo):

| Lado | Precio | \(e_n\) (contribución de ese lado) |
| --- | --- | --- |
| Bid | igual, \(q^B\) sube o baja | \(q^B_n - q^B_{n-1}\) (tamaño añadido o retirado) |
| Bid | sube | \(+ q^B_n\) (tamaño de la orden que mejora el bid) |
| Bid | baja | \(- q^B_{n-1}\) (tamaño retirado: market sell o cancel buy) |
| Ask | igual | \(q^A_{n-1} - q^A_n\) (signo invertido) |
| Ask | baja | \(- q^A_n\) |
| Ask | sube | \(+ q^A_{n-1}\) |

Un market sell y un cancel buy del mismo tamaño son equivalentes: ambos
reducen la cola del bid. Valores positivos = presión compradora neta
(se añade profundidad en bid o se consume ask).

Los indicadores \(\ge\) y \(\le\) se activan **los dos** cuando el precio no
cambia, y la contribución colapsa al delta de tamaño. Cuando el precio
cambia, se carga el tamaño del nivel nuevo o el del nivel viejo, no el
delta. Por eso un salto de un tick puede inyectar un \(|e_n|\) grande
aunque el tamaño en el nuevo nivel sea parecido.

## Agregación a un intervalo: \(\mathrm{OFI}_k\)

Los eventos ocurren en tiempos aleatorios \(\tau_n\). Sea
\(N(t) = \max\{n : \tau_n \le t\}\) el recuento de eventos en \([0, t]\).
Sobre una malla \(\{t_k\}\) (en el paper, \(\Delta t = t_k - t_{k-1} = 10\)
segundos):

\[
\mathrm{OFI}_k
=
\sum_{n = N(t_{k-1})+1}^{N(t_k)} e_n
\]

Es decir: suma de las \(e_n\) cuyo \(\tau_n\) cae en \((t_{k-1}, t_k]\).

## Qué regresiona realmente el paper

Cambio de mid en **el mismo** intervalo, en ticks:

\[
\Delta P_k = (P_k - P_{k-1}) / \delta
\]

donde \(P_k\) es el mid-quote en \(t_k\) y \(\delta\) el tick (1 centavo en
sus datos TAQ).

Modelo estilizado (eq. (1) del preprint): \(\Delta P = \mathrm{OFI}/(2D)+\varepsilon\),
con \(D\) la profundidad por nivel.

Especificación estadística (eq. (2)):

\[
\Delta P_k = \beta \, \mathrm{OFI}_k + \varepsilon_k
\]

OLS que **estiman** (eq. (4), por submuestra de media hora \(i\)):

\[
\Delta P_k = \hat\alpha_i + \hat\beta_i \, \mathrm{OFI}_k + \hat\varepsilon_k
\]

Resultado que citan (resumen y Tabla 2): relación lineal contemporánea,
**\(R^2\) medio del 65 %** en 50 acciones US (S&P 500), TAQ abril 2010,
ventanas de **10 segundos**, \(\beta\) casi siempre significativo, intercepto
casi nunca. Eso es **explicación contemporánea** del \(\Delta P\) del mismo
intervalo, no un test predictivo \(\Delta P_{k+1} \sim \mathrm{OFI}_k\).

Un footnote avisa de posible tautología (los eventos que *mueven* el quote
entran en \(\mathrm{OFI}_k\)). Excluyendo esos eventos el \(R^2\) bajó pero
siguió en 35–60 %.

## Interpretación para market making

OFI contemporáneo es un termómetro de presión en el touch: sirve para
entender *por qué* se movió el mid en esa ventana. La pregunta operativa
(¿puedo usar \(\mathrm{OFI}_t\) para predecir \(\Delta\mathrm{mid}_{t+1}\)?)
es **más dura** y no es lo que el paper mide. La validación empírica de
este repo (`docs/math/ofi_validation.md`) reporta las dos alineaciones.

## Notas de implementación (esta base de código)

Muestreo: Cont et al. muestrean en **tiempos de evento** del libro L1
(cada update de quote TAQ). Aquí cada `BookDelta` de Binance
`@depth@100ms` ya es un diff agregado a 100 ms, no L3 tick-a-tick. Se
evalúa \(e_n\) **tras cada snapshot/delta aplicado** que deje un BBO
válido (no solo cuando cambia L1: un update que no toca el touch da
\(e_n=0\), coherente con la fórmula).

Reglas de omisión (no se forma \(e_n\)):

1. **Primera observación** de una época sincronizada: no hay estado
   \(n-1\). `compute_ofi_events` sobre \(N\) estados devuelve longitud
   \(N-1\). El acumulador streaming hace lo mismo.
2. **Libro no sincronizado** (`not book.is_synced`): se **salta** y se
   **resetea** el estado previo para no cruzar un gap con un \(e_n\)
   espurio.
3. **Libro vacío** o un lado sin BBO: se salta y se resetea.
4. **Tamaños cero** en el touch: se salta (cola ausente; no es un estado
   L1 del paper).
5. Tras un resync (nuevo snapshot REST) empieza una época nueva: la
   primera L1 válida no produce \(e_n\).

Agregación temporal (1 s / 5 s / 10 s): suma de \(e_n\) con \(\tau_n\) en
la ventana. No confundir con `compute_ofi(..., window=W)`, que agrupa
**W eventos** consecutivos (útil en tests; no es la malla del paper).

## Parámetros

| Parámetro | Paper | Aquí |
| --- | --- | --- |
| Niveles | L1 (best bid/ask) | L1 |
| \(\Delta t\) | 10 s (robustez: 10 quotes hasta 10 min) | 1 s, 5 s, 10 s |
| \(\Delta P\) | mid en ticks, **contemporáneo** | contemporáneo **y** lead-1 (siguiente ventana) |
| Tick \(\delta\) | 0.01 USD (equities) | en BTCUSDT el tick es 0.1 USD; la validación reporta \(\Delta\mathrm{mid}\) en precio, no en ticks, y lo declara |
| Profundidad \(D\) | media de \((q^B+q^A)/2\) por intervalo | no se estima \(\beta = c/D^\lambda\) en esta fase |

## Citación

Cont, Rama, Arseniy Kukanov y Sasha Stoikov. 2014. “The Price Impact of
Order Book Events.” *Journal of Financial Econometrics* 12 (1): 47–88.
Preprint: https://arxiv.org/abs/1011.6402 (consultado 2026-09-02).
