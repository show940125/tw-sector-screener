from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CoverageGateResult:
    passed: bool
    required_top_n: int
    coverage_count: int
    attempted_count: int
    ranked_count: int
    missing_count: int
    missing_candidates: tuple[dict[str, Any], ...]
    reason_codes: tuple[str, ...]
    universe_limit_applied: bool
    complete_coverage_required: bool = True
    benchmark_valid: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "required_top_n": self.required_top_n,
            "coverage_count": self.coverage_count,
            "attempted_count": self.attempted_count,
            "ranked_count": self.ranked_count,
            "missing_count": self.missing_count,
            "missing_candidates": [dict(item) for item in self.missing_candidates],
            "reason_codes": list(self.reason_codes),
            "universe_limit_applied": self.universe_limit_applied,
            "complete_coverage_required": self.complete_coverage_required,
            "benchmark_valid": self.benchmark_valid,
        }


class CoverageGateError(RuntimeError):
    def __init__(self, result: CoverageGateResult, artifacts: dict[str, Path]) -> None:
        self.result = result
        self.artifacts = dict(artifacts)
        reasons = ", ".join(result.reason_codes) or "coverage_gate_failed"
        super().__init__(
            f"coverage gate failed: ranked={result.ranked_count}/{result.required_top_n}, "
            f"coverage={result.coverage_count}, attempted={result.attempted_count}, "
            f"missing={result.missing_count}, reasons={reasons}"
        )


def evaluate_coverage_gate(
    *,
    coverage_count: int,
    attempted_count: int,
    ranked_count: int,
    top_n: int,
    missing_candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    universe_limit_applied: bool = False,
    benchmark_valid: bool = True,
) -> CoverageGateResult:
    missing = tuple(dict(item) for item in (missing_candidates or []))
    reasons: list[str] = []
    if coverage_count < top_n:
        reasons.append("coverage_universe_below_top_n")
    if universe_limit_applied or attempted_count < coverage_count:
        reasons.append("universe_limit_applied")
    if missing:
        reasons.append("daily_data_missing")
    if ranked_count < top_n:
        reasons.append("ranked_below_top_n")
    if ranked_count != attempted_count:
        reasons.append("ranked_count_mismatch")
    if not benchmark_valid:
        reasons.append("benchmark_data_missing")
    return CoverageGateResult(
        passed=not reasons,
        required_top_n=top_n,
        coverage_count=coverage_count,
        attempted_count=attempted_count,
        ranked_count=ranked_count,
        missing_count=len(missing),
        missing_candidates=missing,
        reason_codes=tuple(dict.fromkeys(reasons)),
        universe_limit_applied=universe_limit_applied or attempted_count < coverage_count,
        benchmark_valid=benchmark_valid,
    )
