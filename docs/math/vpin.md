# VPIN — Volume-Synchronised Probability of Informed Trading

Implementación: `order_flow.metrics.vpin` (núcleo numpy),
`order_flow.metrics.stream.VpinAccumulator` (streaming, buckets parciales) y
`order_flow.metrics.batch.vpin_from_capture` (Parquet de trades → el mismo núcleo).

**Uso en este sistema:** descriptor **retrospectivo** de toxicidad de flujo, pensado
para ajustar spreads de market making *después* de observar el desequilibrio. **No**
es una señal anticipatoria de crash. Quien lo enchufe a una estrategia debe leer
la sección Andersen–Bondarenko más abajo.

Recuperación de fuentes: **2026-09-02**.

## Fuentes primarias

### Paper de la métrica

Easley, D., López de Prado, M. & O'Hara, M. (2012). *Flow Toxicity and Liquidity
in a High-frequency World*. *Review of Financial Studies*, 25(5), 1457–1493.

- DOI: [10.1093/rfs/hhs053](https://doi.org/10.1093/rfs/hhs053)
- Página oficial (Oxford / RFS): [https://doi.org/10.1093/rfs/hhs053](https://doi.org/10.1093/rfs/hhs053)
- Working paper (Stern NYU, febrero 2012, *forthcoming RFS*), de donde se
  transcribieron las fórmulas: [PDF Stern](https://www.stern.nyu.edu/sites/default/files/assets/documents/con_035928.pdf)

La innovación central **no** es una ventana de tiempo: es el *volume clock*. Se
parte la sesión en *buckets* de volumen fijo \(V\) y se actualiza VPIN cada vez
que se llena un bucket. En un día de volumen medio con \(V =\) ADV/50 y
\(n = 50\) eso equivale, *en promedio*, a un VPIN “diario”; en un día hiperactivo
(p. ej. 6 de mayo de 2010) el mismo \(n\) cubre pocas horas de reloj.

### Bulk Volume Classification (BVC)

En 2012 el BVC aparece como procedimiento auxiliar (sección 2.3). El paper
dedicado es:

Easley, D., López de Prado, M. & O'Hara, M. (2016). *Discerning Information from
Trade Data*. *Journal of Financial Economics*, 120(2), 269–285.

- DOI: [10.1016/j.jfineco.2016.01.018](https://doi.org/10.1016/j.jfineco.2016.01.018)

BVC agrega trades en barras cortas (tiempo o volumen) y asigna una *fracción*
del volumen a compras vía la CDF normal del cambio de precio estandarizado;
Lee–Ready asigna cada print a un lado. En cripto el lado agresor suele venir
en el print (Binance `m`); BVC queda como alternativa cuando no.

## Volume bucketing (no es una ventana de tiempo)

Sea \(V > 0\) el tamaño exógeno del bucket. Se recorren los trades en orden de
evento y se acumula volumen hasta \(V\). **Un trade que cruza el borde se parte
pro-rata**: el exceso va al bucket siguiente (Easley et al. 2012, §2.2:
*“If the last trade needed to complete a bucket is for a size greater than
required, the excess size is given to the next bucket.”*). Un trade mayor que
\(V\) puede completar varios buckets.

El bucket incompleto al final de la muestra es `remainder`; no entra en VPIN.

Volumen cero se ignora. No hay bucket vacío: un bucket existe solo cuando se ha
acumulado exactamente \(V\).

## Clasificación \(V_B\), \(V_S\)

### Agresor (`classification="aggressor"`)

En Binance USD-M, `m=true` significa “el comprador es el maker” → agresor
vendedor, \(s_i = -1\). Entonces, dentro del bucket \(\tau\),

\[
V_{B,\tau} = \sum_i \mathbf{1}\{s_i = +1\}\, v_i^{\tau},\qquad
V_{S,\tau} = V - V_{B,\tau}
\]

donde \(v_i^{\tau}\) es la porción del trade \(i\) que cayó en \(\tau\) (tras el split).

### BVC (`classification="bvc"`)

En el paper de 2012 (eq. (7) del working paper Stern) el BVC se aplica a
**time bars** de un minuto que luego se empaquetan en buckets de volumen:

\[
V^B_{\tau}
= \sum_{i=t(\tau-1)+1}^{t(\tau)}
  V_i \cdot \Phi\!\left(\frac{P_i - P_{i-1}}{\sigma_{\Delta P}}\right),\qquad
V^S_{\tau} = V - V^B_{\tau}
\]

\(\Phi = Z\) es la CDF de la normal estándar; \(\sigma_{\Delta P}\) es la
desviación típica de los cambios de precio entre time bars. Una barra sin
cambio de precio se parte 50/50. El paper dice explícitamente que el análisis
*también* puede hacerse con *volume bars* (nota 12).

**Lo que implementamos:** la variante de *volume bar* (un BVC por bucket
completado), que es lo que el usuario pidió (“price change within each
bucket”) y lo que el paper autoriza:

\[
V_{B,\tau} = V \cdot \Phi\!\left(\frac{\Delta P_{\tau}}{\sigma_{\Delta P}}\right),\qquad
\Delta P_{\tau} = P^{\mathrm{close}}_{\tau} - P^{\mathrm{close}}_{\tau-1}
\]

El primer bucket usa el precio del primer trade como referencia. Si
\(\sigma_{\Delta P}\) no se pasa, se estima como la desviación muestral
(`ddof=1`) de la serie \(\{\Delta P_{\tau}\}\) de **todos** los buckets
completos de la muestra. Si \(\sigma = 0\) o hay un solo bucket, cada bucket
es 50/50 (BVC no tiene señal). En streaming, el mismo \(\sigma\) fijado
reproduce el batch; un \(\sigma\) expansivo *no* coincide con el batch
(el batch usa \(\sigma\) de toda la muestra).

## Fórmula de VPIN

Easley et al. (2012), eq. (9) del working paper. Como cada bucket tiene
volumen \(V\) constante, \(\mathbb{E}[V^B + V^S] = V\) y

\[
\mathrm{VPIN}_{\tau}
= \frac{\alpha\mu}{\alpha\mu + 2\varepsilon}
\approx
\frac{1}{n V}
\sum_{j=\tau-n+1}^{\tau}
\bigl|V_{S,j} - V_{B,j}\bigr|
\in [0, 1]
\]

Es la media móvil de \(n\) buckets del desequilibrio absoluto normalizado.
Por construcción \(0 \le |V_S - V_B| \le V\), así que VPIN \(\in [0, 1]\).
Flujo 100 % compras (o 100 % ventas) → 1; flujo equilibrado en cada bucket
→ 0.

Valores por defecto del paper: \(V =\) ADV/50, \(n = 50\). No los copiamos
como magia: en una captura de ~40 min hay que elegir \(V\) para tener
suficientes buckets, y documentarlo.

## Crítica Andersen–Bondarenko (2014) — lectura obligatoria

Andersen, T. G. & Bondarenko, O. (2014). *VPIN and the Flash Crash*.
*Journal of Financial Markets*, 17, 1–46.

- DOI: [10.1016/j.finmar.2013.05.005](https://doi.org/10.1016/j.finmar.2013.05.005)
- Volumen 17, issue 1, enero 2014, pp. 1–46.

Hallazgo (paráfrasis fiel del abstract, no inventada):

> La investigación empírica documenta que VPIN es **un mal predictor de la
> volatilidad de corto plazo**, que **no alcanzó un máximo histórico antes
> del Flash Crash sino después**, y que su contenido “predictivo” se debe
> sobre todo a una **relación mecánica con la intensidad de negociación**.
> Los autores examinan también la encarnación posterior de VPIN (Easley et
> al. 2012) y llegan a conclusiones similares.

En otras palabras: en el episodio del 6 de mayo de 2010, VPIN **no** dio un
aviso temprano útil; los máximos llegaron *tras* el colapso. Easley, López
de Prado y O'Hara publicaron una réplica en el mismo volumen
(*VPIN and the Flash Crash: A rejoinder*, *J. Financial Markets* 17,
47–52, DOI [10.1016/j.finmar.2013.06.007](https://doi.org/10.1016/j.finmar.2013.06.007)).
El desacuerdo existe; **este sistema se pone del lado cauto**: VPIN se
trata como estadístico descriptivo de toxicidad de flujo, no como alarma
de crash.

**No** se corre un backtest “VPIN predice el crash”. El notebook solo puede
pintar la serie descriptiva sobre la captura de Carlos.

## Cómo lo tratamos aquí

| Sí | No |
| --- | --- |
| Media móvil de \(\lvert V_S - V_B\rvert / V\) en reloj de volumen | Ventana de tiempo disfrazada de VPIN |
| Split pro-rata en los bordes | Tirar el trade que cruza |
| Agresor Binance **y** BVC | Afirmar que BVC “clasifica mejor” en perps cripto sin evidencia propia |
| Comentario *dentro* de `compute_vpin` apuntando a Andersen–Bondarenko | Enchufarlo a un halt / a un predictor de Δmid |

Nombre público preferido: `compute_retrospective_vpin` (alias de
`compute_vpin`). La primera frase del docstring deja claro que **no** es
early-warning.

## API (núcleo compartido)

- `bucket_trades` / `bucket_trades_bvc` → `VolumeBuckets`
- `compute_vpin_from_buckets(buckets, window)` → array en \([0, 1]\)
- `compute_vpin` / `compute_retrospective_vpin`
- Streaming: `VpinAccumulator` (mismo split y la misma fórmula rolling)
- Batch: trades Parquet → agresor almacenado (`aggressor_sign`)

## Referencias

1. Easley, D., López de Prado, M. & O'Hara, M. (2012). Flow Toxicity and
   Liquidity in a High-frequency World. *RFS* 25(5), 1457–1493.
   DOI 10.1093/rfs/hhs053.
2. Easley, D., López de Prado, M. & O'Hara, M. (2016). Discerning Information
   from Trade Data. *JFE* 120(2), 269–285. DOI 10.1016/j.jfineco.2016.01.018.
3. Andersen, T. G. & Bondarenko, O. (2014). VPIN and the Flash Crash.
   *Journal of Financial Markets* 17, 1–46. DOI 10.1016/j.finmar.2013.05.005.
