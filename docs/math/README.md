# Notación de las notas de métricas

Índice `n` = estado del libro (o trade) tras el evento n-ésimo. Ventanas no solapadas de
`W` eventos se numeran `k`. Precios y tamaños del mejor bid/ask: `(P^b, q^b)`, `(P^a, q^a)`.
En MLOFI el superíndice `m` es el nivel (m=1 es el best).

Signo del agresor: `s_i = +1` compra iniciada (taker buy), `-1` venta iniciada. En Binance
el flag `m` (“buyer is the maker”) implica agresor vendedor.

Cada nota cita el paper de donde se tomó la fórmula implementada en `order_flow.metrics`.
Las funciones son numpy puro: no hay I/O ni estado.

| Nota | Módulo |
| --- | --- |
| [ofi.md](ofi.md) | `order_flow.metrics.ofi` |
| [ofi_validation.md](ofi_validation.md) | `scripts/validate_ofi.py` (OLS empírica 1s/5s/10s) |
| [mlofi.md](mlofi.md) | `order_flow.metrics.mlofi` |
| [mlofi_validation.md](mlofi_validation.md) | `scripts/validate_mlofi.py` (L1 vs M5 vs M10) |
| [vpin.md](vpin.md) | `order_flow.metrics.vpin` |
| [cvd.md](cvd.md) | `order_flow.metrics.cvd` |
| [microprice.md](microprice.md) | `OrderBook.microprice()` |
