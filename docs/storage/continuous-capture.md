# Captura continua (días / semanas)

Objetivo: juntar varios días por símbolo (BTC y alts) para que la validación de OFI deje
de depender de una sola ventana de 15–40 min (hallazgo H1 de la auditoría QA).

## Piezas

| Pieza | Qué hace |
| --- | --- |
| `scripts/capture_loop.sh SYMBOL BASE [SECONDS=86400] [SNAPSHOT_INTERVAL=10]` | Una corrida de `record_l2.py` en `BASE/SYMBOL/<UTC>/`, y al terminar `validate_capture.py` (escribe `quality.json`). Reenvía SIGTERM/SIGINT al recorder y mantiene el Mac despierto (`caffeinate -i -w`). |
| `scripts/install_capture_agent.sh SYMBOL BASE [--remove]` | Instala un LaunchAgent de usuario (`com.orderflow.capture.<symbol>`) con `KeepAlive`: al acabar cada corrida de 24 h (o tras un crash, como mucho cada 30 s) arranca la siguiente. |
| `ops/launchd/capture.plist.template` | Plantilla del agente. `ExitTimeOut` 60 s de gracia antes del SIGKILL al desinstalar. |

Una carpeta por corrida: el chequeo de fin de corrida nunca relee días de datos y un
crash pierde, como mucho, una corrida parcial. Cada corrida trae su `instrument.json`,
`capture_meta.json` (incluye `interrupted`, `stale_disconnects`, `queue_high_watermark`,
latencia de sesión completa), `quality.json` y `record.log`.

## Instalar / parar

Desde el checkout que va a usar el agente (no un worktree temporal), con `uv sync` hecho:

```bash
scripts/install_capture_agent.sh BTCUSDT "$PWD/data/continuous"
scripts/install_capture_agent.sh DOGEUSDT "$PWD/data/continuous"
```

```bash
scripts/install_capture_agent.sh BTCUSDT "$PWD/data/continuous" --remove
```

Estado: `launchctl print gui/$(id -u)/com.orderflow.capture.btcusdt`. Logs del agente:
`data/continuous/BTCUSDT/launchd.log`; de cada corrida: `<corrida>/record.log`.

## Analizar varias corridas como una sola cinta

Todo lo que lee Parquet acepta la carpeta base del símbolo (`data/continuous/BTCUSDT`):
`capture_dirs` la expande en sus corridas y se leen como una sola cinta. Las corridas se
unen como resyncs (cada una empieza con un snapshot REST y los update ids siguen
creciendo entre sesiones), así que cada una es una época nueva.

```bash
uv run python scripts/validate_capture.py data/continuous/BTCUSDT
uv run --extra notebooks python scripts/validate_ofi.py --root data/continuous/BTCUSDT
```

- `instrument.json`: se usa el de las corridas si coincide; si el tick cambió entre
  corridas, la lectura falla (una sola rejilla no puede replayar ambas).
- Ventanas de tiempo (OFI / MLOFI 1s/5s/10s): una ventana vacía entre dos épocas es un
  **hueco**, no una observación con OFI = 0 y Δmid = 0. Uniendo las 6 capturas de
  `data/` (2–21 sept) hay ~1.9 M ventanas de 1 s y solo ~5 700 con datos; como ceros
  válidos habrían dominado la OLS.
- Los replays solo construyen los snapshots que aplican (los periódicos se deciden por
  cabecera): cargar esas 6 capturas pasó de 51.5 s a 9.8 s.

## Disco

Medido sobre `data/qa-audit-15min` (BTCUSDT): con flush cada 2 s el recorder escribía
~1 330 archivos cada 15 min (~128 000/día/símbolo, de 4–55 KB) y comprimía ~3.8× peor que
partes grandes. `capture_loop.sh` usa `--flush-interval 60 --buffer-size 50000` y
snapshots periódicos cada 10 s: **~0.5 GB/día para BTCUSDT** (medido en la primera
corrida continua, 2026-09-27), menos en alts.
`record_l2.py` para limpio (meta escrito, `LowDiskSpaceError`) si quedan menos de
`--min-free-gb` (2 GB por defecto).

Costo del flush de 60 s: un corte de luz o `kill -9` pierde como mucho 60 s. SIGTERM
(desinstalar, apagar, `launchctl bootout`) no pierde nada: flush + meta en ~1 s.

## Qué no se hizo, a propósito

- **Rotación make-before-break antes del corte de 24 h de Binance.** Abrir el socket
  nuevo antes de cerrar el viejo produce dos cadenas de update ids solapadas que cada
  consumidor tendría que deduplicar. El corte actual cuesta ~1 s de datos al día, queda
  como una época nueva limpia (resync REST) y `validate_capture` lo cuenta como época, no
  como rotura. No compensa.

## Límites del Mac

`caffeinate -i` evita el reposo por inactividad, pero **no** el reposo al cerrar la tapa
de un portátil sin corriente y sin pantalla externa. Si el Mac duerme, el socket muere:
al despertar el watchdog / reconexión resincroniza y la corrida sigue con una época
nueva. Lo verás como hueco de tiempo en `capture_report.py` y como época extra en
`quality.json`.
