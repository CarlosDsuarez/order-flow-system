# Notebooks

Exploración de métricas y humo de la librería. No son parte del pipeline de captura.

```bash
uv sync --extra notebooks
uv run jupyter lab notebooks/
```

`01_smoke_metrics.ipynb` corre OFI sobre una serie L1 sintética (la misma del test unitario).
La captura L2 en vivo no va aquí: usa `scripts/validate_live_l2.py`.

Validación empírica OFI/CVD (fuente de verdad: el script, no el notebook):

```bash
uv run --extra notebooks python scripts/validate_ofi.py \
  --root data/live-btcusdt-45min --report docs/math/ofi_validation.md
```

`notebooks/ofi_cvd_validation.ipynb` documenta esa corrida y vuelve a invocar el script.

Validación MLOFI (L1 vs M5 vs M10, misma captura) y serie descriptiva de VPIN:

```bash
uv run --extra notebooks python scripts/validate_mlofi.py \
  --root data/live-btcusdt-45min --report docs/math/mlofi_validation.md
```

`notebooks/mlofi_vpin_validation.ipynb` ejecuta el script y pinta VPIN en reloj de volumen
(no es un test de predicción de crash).
