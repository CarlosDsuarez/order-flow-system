# Estructura de datos del libro L2

Fecha del microbench: **2026-09-02**. Máquina: CPython 3.12, `uv run python scripts/bench_orderbook.py`.

El libro en producción (`order_flow.orderbook.book.OrderBook`) usa
`sortedcontainers.SortedDict` (precios `float` como clave). No hay árbol rojo-negro
propio ni crate Rust/C++ en esta fase.

## Presupuesto

- Stream Binance USD-M `@depth@100ms`: ~10 **eventos**/s.
- Cada evento puede tocar 50–200 niveles → **500–2000 operaciones de nivel / s**.
- Microbench también cubre 1k–10k insert/update/delete de un nivel por segundo.
- Libro típico de futuros: ~1000 niveles por lado (`GET /fapi/v1/depth?limit=1000`).

## Alternativas medidas

| Estructura | Qué es | Resultado mixto (insert/update/delete + best) | p99 |
| --- | --- | --- | --- |
| `SortedDict` | skip-list C en `sortedcontainers` | **1.66e6 ops/s** | 1.04 µs |
| `bisect` + dos `list` | alternativa honesta en Python puro | **2.27e6 ops/s** | 1.12 µs |

Burst `SortedDict` (2000 eventos × 100 niveles, libro de 1000 niveles):
**2.43e6 level-ops/s**.

No se implementó un RB-tree de producción: a ~1000 niveles un `list` + `bisect` gana
el microbench (inserción O(n) barata en listas pequeñas). Un RB-tree propio añadiría
código y bugs sin ganar el presupuesto.

## Decisión

**Python + `SortedDict` es suficiente** a volumen Binance `@depth@100ms`. El margen
es de tres órdenes de magnitud (necesitamos ~10³ ops/s; medimos ~10⁶).

Rust/C++ (o `nautilus_trader` / un LOB nativo) queda **aplazado**. La investigación
previa que sugería nativo no se sostiene con estos números en CPython 3.12.

Se conserva `SortedDict` (no se migra a `bisect`+list) porque:

1. Ambas aplastan el presupuesto; el ganador del microbench no importa.
2. `peekitem` / `irange` dan BBO y top-N en O(1)/O(k).
3. O(log n) si el libro crece más allá de 1000 niveles.
4. Ya está cableado y cubierto por tests.

## API relevante (fase 3)

- `best_bid()` / `best_ask()` / `mid_price()` / `spread()` / `microprice()` / `imbalance()`
- `depth_at_level(n)` — **1-indexado**; el nivel 1 es el BBO
- `snapshot()` — `BookSnapshot` con **todos** los niveles en memoria
- `is_synced` — `False` vacío o en gap; `True` tras `apply_snapshot`; el feed llama
  `mark_unsynced()` en resync / disconnect
- `last_update_ts_ns` — alias de `ts_event_ns` del último evento aplicado

## Caveat de precios `float`

Las claves siguen siendo `float` (fase 1). `float("100.50")` es determinista, así que
snapshot y delta coinciden. La aritmética (mid, spread) tiene error de punto flotante.
**Roadmap:** ticks enteros por instrumento; no se migra ahora.
