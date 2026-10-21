from __future__ import annotations

import threading
import time
from collections import Counter


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: Counter[str] = Counter()
        self._request_count = 0
        self._errors: Counter[str] = Counter()
        self._latencies_ms: list[float] = []
        self._started_at = time.time()
        self._recovery_events = 0

    def record(
        self,
        operation: str,
        elapsed_seconds: float,
        *,
        failed: bool = False,
        request: bool = True,
    ) -> None:
        with self._lock:
            self._requests[operation] += 1
            if request:
                self._request_count += 1
            self._latencies_ms.append(elapsed_seconds * 1000)
            if len(self._latencies_ms) > 10000:
                del self._latencies_ms[:5000]
            if failed:
                self._errors[operation] += 1

    def record_recovery(self) -> None:
        with self._lock:
            self._recovery_events += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            latencies = sorted(self._latencies_ms)
            count = self._request_count
            uptime = max(time.time() - self._started_at, 0.001)

            def percentile(fraction: float) -> float:
                if not latencies:
                    return 0.0
                index = min(int((len(latencies) - 1) * fraction), len(latencies) - 1)
                return round(latencies[index], 3)

            return {
                "uptime_seconds": round(uptime, 3),
                "requests_total": count,
                "requests_per_second": round(count / uptime, 3),
                "errors": dict(self._errors),
                "operations": dict(self._requests),
                "latency_ms_p50": percentile(0.50),
                "latency_ms_p95": percentile(0.95),
                "recovery_events": self._recovery_events,
            }
