# Fixtures de test

## `golden_btcusdt_resync/`

Cinta **real** (Binance USD-M BTCUSDT, mainnet público, sin órdenes) recortada de
`data/mi-captura` (no versionada): 60 s de event time a partir del primer snapshot
grabado alrededor del resync `lastUpdateId = 11618318685647` (2026-09-21).

Por qué esta ventana: contiene el patrón que rompió los replays dos veces, un snapshot
REST que comparte `u` con el diff que lo bracket-ea, con el `E` del REST posterior al
del diff y el diff trayendo niveles más profundos que los 1 000 del REST.

Contenido: 12 snapshots (el REST y 1 de cada 5 periódicos), 589 deltas y 3 376
trades, un flush por tipo (cada archivo < 500 KB, límite del hook
`check-added-large-files`). `instrument.json` es la spec legacy de BTCUSDT (tick 0.1,
lote 0.001): la captura original es anterior a `instrument.json`.

Los deltas se recortan por update id (`u >= lastUpdateId` del REST), no por tiempo: el
diff que bracket-ea el REST tiene `E` 2 ms **antes** que el REST, y un recorte por
tiempo lo perdía junto con sus niveles profundos (el test de la cinta real lo detectó).

`tests/unit/test_golden_tape.py` fija sus resultados (integridad, replays, OFI). Si un
cambio intencional los mueve, actualiza los números **y** explica el porqué en el commit.
