"""Layer 3 - microstructure metrics as pure numpy functions.

Each module documents its formula and primary source; the Spanish derivations live in
``docs/math/``. All functions accept array-likes and return ``float64`` numpy arrays.
"""

from order_flow.metrics.cvd import (
    CvdBars,
    aggressor_sign_from_binance_m,
    compute_cvd,
    compute_trade_delta,
    resample_cvd,
    sides_to_signs,
)
from order_flow.metrics.mlofi import (
    DEFAULT_MLOFI_LEVELS,
    MlofiWindowFrame,
    aggregate_mlofi_levels,
    compute_mlofi,
    compute_mlofi_events,
    compute_mlofi_time_windows,
    level_weights,
)
from order_flow.metrics.ofi import (
    OfiWindowFrame,
    compute_ofi,
    compute_ofi_events,
    compute_ofi_time_windows,
)
from order_flow.metrics.vpin import (
    VolumeBuckets,
    bucket_trades,
    bucket_trades_bvc,
    compute_retrospective_vpin,
    compute_vpin,
    compute_vpin_from_buckets,
)

__all__ = [
    "DEFAULT_MLOFI_LEVELS",
    "CvdBars",
    "MlofiWindowFrame",
    "OfiWindowFrame",
    "VolumeBuckets",
    "aggregate_mlofi_levels",
    "aggressor_sign_from_binance_m",
    "bucket_trades",
    "bucket_trades_bvc",
    "compute_cvd",
    "compute_mlofi",
    "compute_mlofi_events",
    "compute_mlofi_time_windows",
    "compute_ofi",
    "compute_ofi_events",
    "compute_ofi_time_windows",
    "compute_retrospective_vpin",
    "compute_trade_delta",
    "compute_vpin",
    "compute_vpin_from_buckets",
    "level_weights",
    "resample_cvd",
    "sides_to_signs",
]
