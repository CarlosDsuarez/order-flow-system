# Microprice

Implementación: `OrderBook.microprice()`.

## Fuentes

- Stoikov, S. (2018). *The Micro-Price: A High-Frequency Estimator of Future Prices*.
  https://arxiv.org/abs/1503.05671 (el micro-price “completo” de Stoikov es un predictor
  por desequilibrio; aquí se usa el mid ponderado por tamaño, el término de orden cero).
- Gatheral, J. & Oomen, R. C. A. (2010). *Zero-intelligence realized variance estimation*.
  Finance and Stochastics, 14, 249–283. (mid ponderado por tamaño en el top of book.)

## Fórmula

Con best bid \((P^b, q^b)\) y best ask \((P^a, q^a)\):

\[
P^{\mathrm{micro}} = \frac{P^a q^b + P^b q^a}{q^a + q^b}
\]

Es el mid clásico cuando \(q^a = q^b\), y se desplaza hacia el lado con **menos** tamaño
(el lado que se espera que se mueva). Requiere ambos lados no vacíos (`EmptyBookError`
si no). Precio en `float` en esta fase; el roadmap pasa a ticks enteros.
