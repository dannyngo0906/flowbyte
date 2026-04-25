"""SyncRunner: orchestrates Extract → Transform → Load for one resource."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from flowbyte.config.models import PipelineConfig, SyncJobSpec, SyncResult
from flowbyte.db.destination_schema import destination_metadata, get_table
from flowbyte.db.internal_schema import sync_runs
from flowbyte.haravan.client import HaravanClient
from flowbyte.logging import EventName, get_logger
from flowbyte.sync.checkpoint import compute_watermark, load_checkpoint, save_checkpoint
from flowbyte.sync.load import LoadStats, count_active_rows, sweep_soft_deletes, upsert_batch
from flowbyte.sync.transform import apply_transform

log = get_logger()

_SOFT_DELETE_SANITY_MAX_PCT = 5.0


class SyncRunner:
    def __init__(
        self,
        pipeline_config: PipelineConfig,
        haravan_client: HaravanClient,
        internal_engine: Engine,
        dest_engine: Engine,
    ) -> None:
        self._cfg = pipeline_config
        self._haravan = haravan_client
        self._internal = internal_engine
        self._dest = dest_engine
        # Ensure destination tables exist (idempotent — CREATE TABLE IF NOT EXISTS)
        destination_metadata.create_all(bind=dest_engine, checkfirst=True)

    def run(self, spec: SyncJobSpec) -> SyncResult:
        sync_id = spec.sync_id or str(uuid4())
        started_at = datetime.now(timezone.utc)
        bound_log = log.bind(sync_id=sync_id, pipeline=spec.pipeline, resource=spec.resource)

        bound_log.info(EventName.SYNC_STARTED, mode=spec.mode, trigger=spec.trigger)
        self._record_run_start(sync_id, spec, started_at)

        result = SyncResult(
            sync_id=sync_id,
            pipeline=spec.pipeline,
            resource=spec.resource,
            mode=spec.mode,
        )

        try:
            if spec.resource == "inventory_levels":
                self._sync_inventory_levels(spec, sync_id, result)
            elif spec.resource == "locations":
                self._sync_locations(spec, sync_id, result)
            elif spec.resource == "products":
                self._sync_products(spec, sync_id, result)
            else:
                self._sync_incremental_resource(spec, sync_id, result)

            result.status = "success"
            bound_log.info(
                EventName.SYNC_COMPLETED,
                fetched=result.fetched_count,
                upserted=result.upserted_count,
                skipped=result.skipped_invalid,
                duration=round(time.time() - started_at.timestamp(), 2),
            )
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            bound_log.error(EventName.SYNC_FAILED, error=str(e), exc_info=True)
        finally:
            result.duration_seconds = round(time.time() - started_at.timestamp(), 2)
            self._record_run_finish(sync_id, result)

        if result.status == "success":
            self._run_validation(result)

        return result

    # ── Resource-specific sync strategies ─────────────────────────────────────

    def _sync_incremental_resource(
        self, spec: SyncJobSpec, sync_id: str, result: SyncResult
    ) -> None:
        table = get_table(spec.resource)

        with self._internal.begin() as int_conn:
            checkpoint = load_checkpoint(int_conn, spec.pipeline, spec.resource)

        # First sync ever → full refresh, then switch to incremental
        effective_mode = spec.mode
        if checkpoint is None and spec.mode == "incremental":
            effective_mode = "full_refresh"
            log.info(EventName.CHECKPOINT_LOADED, reason="first_sync_switching_to_full_refresh")

        records_raw: list[dict] = []
        from flowbyte.haravan.resources.orders import extract_orders
        from flowbyte.haravan.resources.customers import extract_customers

        extractors = {
            "orders": extract_orders,
            "customers": extract_customers,
        }
        extractor = extractors.get(spec.resource)
        if extractor is None:
            raise NotImplementedError(f"No extractor for resource {spec.resource!r}")

        for raw in extractor(self._haravan, checkpoint, effective_mode):
            records_raw.append(raw)

        transformed, skipped = self._transform_batch(records_raw, spec.resource)
        result.fetched_count = len(records_raw)
        result.skipped_invalid = skipped

        with self._dest.connect() as dest_conn:
            result.rows_before = count_active_rows(dest_conn, table)

        with self._dest.begin() as dest_conn:
            stats = upsert_batch(dest_conn, table, transformed, sync_id)
            result.upserted_count = stats.upserted

            if effective_mode == "full_refresh":
                self._run_soft_delete_sweep(dest_conn, table, sync_id, result)

        with self._dest.connect() as dest_conn:
            result.rows_after = count_active_rows(dest_conn, table)

        # Checkpoint saved AFTER destination commit (invariant §8.3)
        watermark = compute_watermark(transformed)
        with self._internal.begin() as int_conn:
            save_checkpoint(int_conn, spec.pipeline, spec.resource, watermark, sync_id)

    def _sync_products(
        self, spec: SyncJobSpec, sync_id: str, result: SyncResult
    ) -> None:
        from flowbyte.haravan.resources.products import extract_products_and_variants

        table = get_table("products")

        with self._internal.begin() as int_conn:
            checkpoint = load_checkpoint(int_conn, spec.pipeline, "products")

        effective_mode = spec.mode
        if checkpoint is None and spec.mode == "incremental":
            effective_mode = "full_refresh"

        products_raw = list(extract_products_and_variants(
            self._haravan, checkpoint, effective_mode
        ))

        products_transformed, skipped = self._transform_batch(products_raw, "products")
        result.fetched_count = len(products_raw)
        result.skipped_invalid = skipped

        with self._dest.connect() as dest_conn:
            result.rows_before = count_active_rows(dest_conn, table)

        with self._dest.begin() as dest_conn:
            p_stats = upsert_batch(dest_conn, table, products_transformed, sync_id)
            result.upserted_count = p_stats.upserted

            if effective_mode == "full_refresh":
                self._run_soft_delete_sweep(
                    dest_conn, table, sync_id, result,
                    effective_fetched=len(products_transformed),
                )

        with self._dest.connect() as dest_conn:
            result.rows_after = count_active_rows(dest_conn, table)

        watermark = compute_watermark(products_transformed)
        with self._internal.begin() as int_conn:
            save_checkpoint(int_conn, spec.pipeline, "products", watermark, sync_id)

    def _sync_inventory_levels(
        self, spec: SyncJobSpec, sync_id: str, result: SyncResult
    ) -> None:
        from flowbyte.haravan.resources.inventory import extract_inventory_levels

        location_ids = self._get_location_ids()
        variant_ids = self._get_variant_ids()
        if not variant_ids:
            log.warning("sync.skipped", resource="inventory_levels", reason="no_variant_ids")
            return

        table = get_table("inventory_levels")

        records_raw = list(extract_inventory_levels(self._haravan, location_ids, variant_ids))
        transformed, skipped = self._transform_batch(records_raw, "inventory_levels")
        result.fetched_count = len(records_raw)
        result.skipped_invalid = skipped

        with self._dest.connect() as dest_conn:
            result.rows_before = count_active_rows(dest_conn, table)

        with self._dest.begin() as dest_conn:
            stats = upsert_batch(
                dest_conn, table, transformed, sync_id,
                primary_key=["inventory_item_id", "location_id"],
            )
            result.upserted_count = stats.upserted

        with self._dest.connect() as dest_conn:
            result.rows_after = count_active_rows(dest_conn, table)

    def _sync_locations(
        self, spec: SyncJobSpec, sync_id: str, result: SyncResult
    ) -> None:
        from flowbyte.haravan.resources.locations import extract_locations

        table = get_table("locations")
        records_raw = list(extract_locations(self._haravan))
        transformed, skipped = self._transform_batch(records_raw, "locations")
        result.fetched_count = len(records_raw)
        result.skipped_invalid = skipped

        with self._dest.connect() as dest_conn:
            result.rows_before = count_active_rows(dest_conn, table)

        with self._dest.begin() as dest_conn:
            stats = upsert_batch(dest_conn, table, transformed, sync_id)
            result.upserted_count = stats.upserted

            self._run_soft_delete_sweep(dest_conn, table, sync_id, result)

        with self._dest.connect() as dest_conn:
            result.rows_after = count_active_rows(dest_conn, table)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _transform_batch(
        self,
        records: list[dict],
        resource: str,
    ) -> tuple[list[dict], int]:
        transformed = []
        skipped = 0
        for r in records:
            row = apply_transform(r, resource)
            if row is None:
                skipped += 1
            else:
                transformed.append(row)
        return transformed, skipped

    def _run_soft_delete_sweep(
        self,
        dest_conn,
        table,
        sync_id: str,
        result: SyncResult,
        effective_fetched: int | None = None,
    ) -> None:
        rows_before = count_active_rows(dest_conn, table)
        fetched = effective_fetched if effective_fetched is not None else result.fetched_count
        would_delete = rows_before - fetched
        if rows_before > 0:
            delete_pct = would_delete / rows_before * 100
            if delete_pct > _SOFT_DELETE_SANITY_MAX_PCT:
                log.critical(
                    EventName.SOFT_DELETE_SWEEP_ABORTED,
                    delete_pct=round(delete_pct, 2),
                    rows_before=rows_before,
                    fetched=fetched,
                )
                return

        swept = sweep_soft_deletes(dest_conn, table, sync_id)
        result.soft_deleted_count = swept
        log.info(EventName.SOFT_DELETE_SWEEP_DONE, swept=swept)

    def _get_location_ids(self) -> list[int]:
        from flowbyte.haravan.resources.locations import extract_locations

        return [loc["id"] for loc in extract_locations(self._haravan) if loc.get("id")]

    def _get_variant_ids(self) -> list[int]:
        from sqlalchemy import text

        try:
            with self._dest.connect() as conn:
                rows = conn.execute(text(
                    "SELECT DISTINCT CAST(v->>'id' AS BIGINT) "
                    "FROM products, jsonb_array_elements(_raw->'variants') AS v "
                    "WHERE _deleted_at IS NULL AND v->>'id' IS NOT NULL"
                )).all()
            return [r[0] for r in rows if r[0]]
        except Exception as e:
            log.warning("sync.skipped", resource="inventory_levels",
                        reason="variant_ids_query_failed", error=str(e))
            return []

    def _run_validation(self, result: SyncResult) -> None:
        try:
            from flowbyte.validation.executor import ValidationExecutor
            executor = ValidationExecutor(self._internal)
            vr_list = executor.run(result)
            result.validation_statuses = [v.status for v in vr_list]
            result.validation_failed = any(v.status == "failed" for v in vr_list)
            result.validation_failed_rules = [v.rule for v in vr_list if v.status == "failed"]
        except Exception as e:
            log.error(EventName.VALIDATION_FAILED, phase="validation_executor", error=str(e), exc_info=True)


    def _record_run_start(
        self, sync_id: str, spec: SyncJobSpec, started_at: datetime
    ) -> None:
        with self._internal.begin() as conn:
            conn.execute(
                pg_insert(sync_runs).values(
                    sync_id=sync_id,
                    pipeline=spec.pipeline,
                    resource=spec.resource,
                    mode=spec.mode,
                    trigger=spec.trigger,
                    request_id=spec.request_id,
                    started_at=started_at,
                    status="running",
                ).on_conflict_do_nothing()
            )

    def _record_run_finish(self, sync_id: str, result: SyncResult) -> None:
        with self._internal.begin() as conn:
            conn.execute(
                sync_runs.update()
                .where(sync_runs.c.sync_id == sync_id)
                .values(
                    finished_at=datetime.now(timezone.utc),
                    status=result.status,
                    fetched_count=result.fetched_count,
                    upserted_count=result.upserted_count,
                    skipped_invalid=result.skipped_invalid,
                    soft_deleted_count=result.soft_deleted_count,
                    rows_before=result.rows_before,
                    rows_after=result.rows_after,
                    duration_seconds=result.duration_seconds,
                    error=result.error,
                )
            )
