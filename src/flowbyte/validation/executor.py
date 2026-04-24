"""ValidationExecutor: runs post-sync validation rules and persists results."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Engine

from flowbyte.config.models import SyncResult
from flowbyte.db.internal_schema import sync_runs, validation_results
from flowbyte.logging import EventName, get_logger
from flowbyte.validation.rules import ValidationContext, ValidationResult, run_all_validations

log = get_logger()

_PREV_RUNS_LIMIT = 7


class ValidationExecutor:
    def __init__(self, internal_engine: Engine) -> None:
        self._engine = internal_engine

    def run(self, result: SyncResult) -> list[ValidationResult]:
        prev_runs = self._load_prev_runs(result.pipeline, result.resource)
        ctx = ValidationContext(
            pipeline=result.pipeline,
            resource=result.resource,
            sync_id=result.sync_id,
            mode=result.mode,
            fetched_count=result.fetched_count,
            upserted_count=result.upserted_count,
            rows_before=result.rows_before,
            rows_after=result.rows_after,
            prev_runs=prev_runs,
        )
        vr_list = run_all_validations(ctx)
        self._persist(ctx, vr_list)
        return vr_list

    def _load_prev_runs(self, pipeline: str, resource: str) -> list[dict]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(sync_runs)
                .where(
                    sync_runs.c.pipeline == pipeline,
                    sync_runs.c.resource == resource,
                    sync_runs.c.status == "success",
                )
                .order_by(sync_runs.c.started_at.desc())
                .limit(_PREV_RUNS_LIMIT)
            ).all()
        return [dict(row._mapping) for row in rows]

    def _persist(self, ctx: ValidationContext, results: list[ValidationResult]) -> None:
        try:
            with self._engine.begin() as conn:
                for vr in results:
                    conn.execute(
                        validation_results.insert().values(
                            sync_id=ctx.sync_id,
                            pipeline=ctx.pipeline,
                            resource=ctx.resource,
                            rule=vr.rule,
                            status=vr.status,
                            details=vr.details or {},
                        )
                    )
            log.info(
                EventName.VALIDATION_DONE,
                pipeline=ctx.pipeline,
                resource=ctx.resource,
                sync_id=ctx.sync_id,
                rules_run=len(results),
                failed=[v.rule for v in results if v.status == "failed"],
            )
        except Exception as e:
            log.error(EventName.VALIDATION_FAILED, error=str(e), exc_info=True)
