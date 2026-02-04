from __future__ import annotations

from collections import defaultdict

_METRICS: dict[str, int] = defaultdict(int)
_TIMINGS_MS: dict[str, float] = defaultdict(float)


def record_request(method: str, path: str, status: int, elapsed_ms: float) -> None:
    key = f"requests_total{{method='{method}',path='{path}',status='{status}'}}"
    _METRICS[key] += 1
    _TIMINGS_MS[f"request_ms_sum{{path='{path}'}}"] += elapsed_ms


def snapshot_text() -> str:
    lines = []
    for k, v in sorted(_METRICS.items()):
        lines.append(f"{k} {v}")
    for k, v in sorted(_TIMINGS_MS.items()):
        lines.append(f"{k} {v:.2f}")
    return "\n".join(lines)


def snapshot_dict() -> dict[str, float | int]:
    out: dict[str, float | int] = {}
    for k, v in _METRICS.items():
        out[k] = v
    for k, v in _TIMINGS_MS.items():
        out[k] = round(v, 2)
    return out
