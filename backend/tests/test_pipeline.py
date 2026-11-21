"""跨模块数据流业务行为测试：covering version tracking, failure recovery,
batch deduplication, watermark management, schema version evolution, and
cross-service data consumption chains (source_registry → ingest_batch →
raw_records → field_parser → parsed_entity → dedup_engine →
aggregation_view → data_export)."""
from __future__ import annotations

import json
import pytest

from app.store import store


class TestSourceRegistry:
    """数据源登记与验证测试。"""

    def test_register_new_source(self, pipeline):
        result = pipeline.register_source("app-error-log", "http", "beijing")
        assert result["ok"] is True
        assert result["entry"]["来源标识"] == "app-error-log"
        assert result["entry"]["status"] == "待验证"
        assert result["entry"]["source_version"] == 1

    def test_register_duplicate_source(self, pipeline):
        pipeline.register_source("duplicate-src", "http", "shanghai")
        result = pipeline.register_source("duplicate-src", "grpc", "beijing")
        assert result["ok"] is False
        assert "已登记" in result["message"]

    def test_validate_existing_source(self, pipeline):
        pipeline.register_source("to-validate", "http", "default")
        result = pipeline.validate_source("to-validate")
        assert result["ok"] is True
        assert result["entry"]["status"] == "已验证"
        assert result["entry"]["source_version"] == 2

    def test_validate_nonexistent_source(self, pipeline):
        result = pipeline.validate_source("ghost-source")
        assert result["ok"] is False

    def test_validate_increments_version(self, pipeline):
        pipeline.register_source("version-src", "http", "default")
        pipeline.validate_source("version-src")
        entry = pipeline.validate_source("version-src")["entry"]
        assert entry["source_version"] >= 2


class TestIngestBatch:
    """采集批次启动测试。"""

    def test_start_batch_unregistered_source(self, pipeline):
        result = pipeline.start_ingest_batch("missing-src", "2026Q1", [])
        assert result["ok"] is False

    def test_start_batch_unvalidated_source(self, pipeline):
        pipeline.register_source("unvalidated-src", "http", "default")
        result = pipeline.start_ingest_batch("unvalidated-src", "2026Q1", [])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_start_batch_success(self, pipeline, sample_raw_logs):
        pipeline.register_source("batch-src", "http", "default")
        pipeline.validate_source("batch-src")
        result = pipeline.start_ingest_batch("batch-src", "2026-01-15", sample_raw_logs)
        assert result["ok"] is True
        assert result["record_count"] == 5
        assert result["batch"]["status"] == "采集中"

    def test_batch_creates_raw_records(self, pipeline, sample_raw_logs):
        pipeline.register_source("record-src", "http", "default")
        pipeline.validate_source("record-src")
        result = pipeline.start_ingest_batch("record-src", "2026-01-15", sample_raw_logs)
        batch_no = result["batch"]["批次编号"]
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert len(batch_raw) == 5
        for r in batch_raw:
            assert r["status"] == "待解析"

    def test_no_duplicate_active_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("single-src", "http", "default")
        pipeline.validate_source("single-src")
        pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[:2])
        result = pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[2:])
        assert result["ok"] is False
        assert "进行中的批次" in result["message"]

    def test_batch_records_source_version(self, pipeline, sample_raw_logs):
        pipeline.register_source("ver-track-src", "http", "default")
        pipeline.validate_source("ver-track-src")
        result = pipeline.start_ingest_batch("ver-track-src", "2026Q1", sample_raw_logs[:1])
        assert result["batch"]["source_version"] == 2


class TestFieldParser:
    """字段解析测试。"""

    def test_parse_batch_no_records(self, pipeline):
        result = pipeline.parse_batch("NONEXIST-BATCH")
        assert result["ok"] is False

    def test_parse_batch_no_parser_rule_uses_default(self, pipeline, sample_raw_logs):
        pipeline.register_source("parse-src", "http", "default")
        pipeline.validate_source("parse-src")
        batch = pipeline.start_ingest_batch("parse-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]

        result = pipeline.parse_batch(batch_no)
        assert result["ok"] is True
        assert result["success"] > 0

    def test_parse_batch_sets_raw_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("status-src", "http", "default")
        pipeline.validate_source("status-src")
        batch = pipeline.start_ingest_batch("status-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert all(r["status"] == "已解析" for r in batch_raw)

    def test_parse_creates_parsed_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("ent-src", "http", "default")
        pipeline.validate_source("ent-src")
        batch = pipeline.start_ingest_batch("ent-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 2
        for p in parsed:
            assert p["status"] == "待校验"

    def test_parse_journals_created(self, pipeline, sample_raw_logs):
        pipeline.register_source("journal-src", "http", "default")
        pipeline.validate_source("journal-src")
        batch = pipeline.start_ingest_batch("journal-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        before = len(store.rows("parse_journal"))
        pipeline.parse_batch(batch_no)
        after = len(store.rows("parse_journal"))
        assert after > before


class TestValidation:
    """质量校验测试。"""

    def test_validate_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("valid-src", "http", "default")
        pipeline.validate_source("valid-src")
        batch = pipeline.start_ingest_batch("valid-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.validate_entities(batch_no)
        assert result["ok"] is True
        assert result["passed"] == 2

    def test_validate_sets_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("vstat-src", "http", "default")
        pipeline.validate_source("vstat-src")
        batch = pipeline.start_ingest_batch("vstat-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert all(p["status"] == "校验通过" for p in parsed)


class TestDeduplication:
    """去重引擎测试。"""

    def test_dedup_no_duplicates(self, pipeline, sample_raw_logs):
        pipeline.register_source("nodup-src", "http", "default")
        pipeline.validate_source("nodup-src")
        batch = pipeline.start_ingest_batch("nodup-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["time", "level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] == 0

    def test_dedup_with_duplicates(self, pipeline):
        pipeline.register_source("dup-src", "http", "default")
        pipeline.validate_source("dup-src")
        # 提交相同日志
        same_logs = [
            {"body": "t1|INFO|msg1"},
            {"body": "t1|INFO|msg1"},  # duplicate
            {"body": "t1|WARN|msg2"},
        ]
        batch = pipeline.start_ingest_batch("dup-src", "2026Q1", same_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] > 0

    def test_dedup_keep_last(self, pipeline):
        pipeline.register_source("kl-src", "http", "default")
        pipeline.validate_source("kl-src")
        logs = [
            {"body": "t1|INFO|first"},
            {"body": "t1|INFO|second"},
        ]
        batch = pipeline.start_ingest_batch("kl-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_last")
        assert result["ok"] is True


class TestAggregation:
    """聚合视图测试。"""

    def test_refresh_aggregation(self, pipeline, sample_raw_logs):
        pipeline.register_source("agg-src", "http", "default")
        pipeline.validate_source("agg-src")
        batch = pipeline.start_ingest_batch("agg-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("test-view", "level", "time", "count")
        assert result["ok"] is True
        assert "view" in result
        assert result["dimensions"] > 0

    def test_aggregation_dimensions(self, pipeline, sample_raw_logs):
        pipeline.register_source("dim-src", "http", "default")
        pipeline.validate_source("dim-src")
        batch = pipeline.start_ingest_batch("dim-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("dim-view", "level", "time", "count")
        results = result["view"]["results"]
        assert "INFO" in results
        assert results["INFO"]["count"] == 2  # two INFO logs


class TestDataExport:
    """数据导出与血缘测试。"""

    def test_build_export(self, pipeline, sample_raw_logs):
        pipeline.register_source("export-src", "http", "default")
        pipeline.validate_source("export-src")
        batch = pipeline.start_ingest_batch("export-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        result = pipeline.build_export(f"view-{batch_no}", "csv")
        assert result["ok"] is True
        assert result["export"]["status"] == "已完成"

    def test_export_creates_lineage(self, pipeline, sample_raw_logs):
        pipeline.register_source("lin-src", "http", "default")
        pipeline.validate_source("lin-src")
        batch = pipeline.start_ingest_batch("lin-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        lineage_before = len(store.rows("data_lineage"))
        pipeline.build_export(f"view-{batch_no}", "csv")
        lineage_after = len(store.rows("data_lineage"))
        assert lineage_after > lineage_before
        last = store.rows("data_lineage")[-1]
        assert last["export_format"] == "csv"


class TestWatermarkManagement:
    """水位管理测试。"""

    def test_advance_watermark_new_source(self, pipeline):
        result = pipeline.advance_watermark("wm-src-1", "INGE-0001", "2026-01-01T00:00:00")
        assert result["ok"] is True
        assert result["watermark"]["数据源"] == "wm-src-1"

    def test_advance_watermark_progress(self, pipeline):
        pipeline.advance_watermark("wm-src-2", "INGE-0001", "2026-01-01T00:00:00")
        result = pipeline.advance_watermark("wm-src-2", "INGE-0002", "2026-01-02T00:00:00")
        assert result["ok"] is True

    def test_watermark_rollback_prevention(self, pipeline):
        pipeline.advance_watermark("wm-src-3", "INGE-0001", "2026-01-05T00:00:00")
        result = pipeline.advance_watermark("wm-src-3", "INGE-0002", "2026-01-01T00:00:00")
        assert result["ok"] is False
        assert "倒退" in result["message"]
        wm = result["message"]
        assert "阻止" in wm

    def test_watermark_rollback_flags_abnormal(self, pipeline):
        pipeline.advance_watermark("wm-src-4", "INGE-0001", "2026-02-01T00:00:00")
        pipeline.advance_watermark("wm-src-4", "INGE-0002", "2026-01-01T00:00:00")
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-src-4"][0]
        assert wm["status"] == "异常回退"
        assert wm["abnormal"] is True


class TestSchemaEvolution:
    """Schema 版本演进测试。"""

    def test_evolve_schema_new(self, pipeline):
        result = pipeline.evolve_schema("v2.0", "app-logs", ["time", "level", "message", "host"], True)
        assert result["ok"] is True
        assert result["schema"]["版本号"] == "v2.0"
        assert result["schema"]["status"] == "已发布"

    def test_evolve_schema_duplicate(self, pipeline):
        pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        result = pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        assert result["ok"] is False
        assert "已存在" in result["message"]

    def test_evolve_schema_non_backward_compatible(self, pipeline):
        result = pipeline.evolve_schema("v4.0", "db-logs", ["timestamp", "query", "duration"], False)
        assert result["ok"] is True
        assert result["schema"]["向后兼容标记"] == "否"


class TestReplayEngine:
    """回溯重放引擎测试。"""

    def test_replay_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("replay-src", "http", "default")
        pipeline.validate_source("replay-src")
        batch = pipeline.start_ingest_batch("replay-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.replay_batch(batch_no, "new-rule-v2")
        assert result["ok"] is True
        assert result["replayed_count"] > 0
        assert result["replay"]["status"] == "已完成"


class TestFailureRecovery:
    """失败恢复测试。"""

    def test_recover_batch(self, pipeline):
        pipeline.register_source("recv-src", "http", "default")
        pipeline.validate_source("recv-src")
        logs = [
            {"body": "bad|format"},  # 只有单字段，解析会失败
            {"body": "t1|INFO|msg"},
        ]
        batch = pipeline.start_ingest_batch("recv-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.recover_batch(batch_no)
        assert result["ok"] is True
        assert isinstance(result["recovered"], int)


class TestFullPipeline:
    """全链路流水线测试。"""

    def test_run_full_pipeline(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "full-pipeline-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        assert result["ok"] is True
        steps = result["steps"]
        assert "source_registry" in steps
        assert "ingest_batch" in steps
        assert "field_parser" in steps
        assert "validate" in steps
        assert "dedup" in steps
        assert "aggregation" in steps
        assert "export" in steps
        assert "watermark" in steps

    def test_full_pipeline_data_lineage(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "lineage-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        lineages = store.rows("data_lineage")
        assert len(lineages) > 0

    def test_full_pipeline_parsed_entities_refer_to_batch(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "ref-check-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        batch_no = result["batch_no"]
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 3

    def test_full_pipeline_watermark_advanced(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "wm-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-check-src"]
        assert len(wm) == 1
        assert wm[0]["status"] == "正常"


class TestCrossModuleSnapshot:
    """跨模块数据快照测试。"""

    def test_snapshot_contains_all_modules(self, pipeline, sample_raw_logs):
        pipeline.run_full_pipeline("snap-src", "2026Q1", sample_raw_logs[:3])

        from app.services.base import BaseModuleService
        from app.services import SERVICES
        snapshots = {key: svc.domain_snapshot() for key, svc in SERVICES.items()}

        assert "source_registry" in snapshots
        assert "ingest_batch" in snapshots
        assert "raw_records" in snapshots
        assert "parsed_entity" in snapshots
        assert "dedup_engine" in snapshots
        assert "aggregation_view" in snapshots
        assert "data_export" in snapshots
        assert "data_lineage" in snapshots
        assert "watermark_mgr" in snapshots

    def test_snapshot_data_consistency(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline("consistency-src", "2026Q1", sample_raw_logs[:3])
        batch_no = result["batch_no"]

        from app.services import SERVICES
        parsed_snap = SERVICES["parsed_entity"].domain_snapshot()
        assert parsed_snap["count"] >= 3

        raw_snap = SERVICES["raw_records"].domain_snapshot()
        assert raw_snap["count"] > 0

class TestDeliveryReceipts:
    """交付入口与聚合刷新取数链路的回执语义测试。"""

    def _prepare_validated_batch(self, pipeline, sample_raw_logs, source="deliver-src"):
        pipeline.register_source(source, "http", "default")
        pipeline.validate_source(source)
        batch = pipeline.start_ingest_batch(source, "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        return batch_no

    def test_export_unseen_view_fails(self, pipeline):
        """日志视图从未生成时，交付入口不能返回成功。"""
        result = pipeline.build_export("never-seen-view")
        assert result["ok"] is False
        assert result["reason"] == "view_not_found"
        assert result["receipt"] is None
        assert not [
            e for e in store.rows("data_export")
            if e.get("导出范围") == "never-seen-view"
        ]

    def test_unseen_view_receipt_is_none(self, pipeline):
        assert pipeline.aggregation_receipt("never-seen-view") is None

    def test_empty_group_refresh_is_valid_receipt(self, pipeline, sample_raw_logs):
        """零行匹配但刷新完成，应得到有效的空分组数据回执。"""
        self._prepare_validated_batch(pipeline, sample_raw_logs)
        result = pipeline.refresh_aggregation(
            "empty-view", "level", "time", "count",
            source_batch="BATCH-WITH-NO-ROWS",
        )
        assert result["ok"] is True
        assert result["refresh_state"] == "completed"
        assert result["dimensions"] == 0

        receipt = pipeline.aggregation_receipt("empty-view")
        assert receipt["refresh_state"] == "completed"
        assert receipt["confirmed"] is True
        assert receipt["empty_group"] is True
        assert receipt["scope"]["requested_batches"] == ["BATCH-WITH-NO-ROWS"]
        assert receipt["scope"]["record_count"] == 0

    def test_empty_group_export_is_confirmed_with_scope(self, pipeline, sample_raw_logs):
        """空分组交付必须写明取数范围，能核对空文件来源。"""
        self._prepare_validated_batch(pipeline, sample_raw_logs)
        pipeline.refresh_aggregation(
            "empty-export", "level", "time", "count",
            source_batch="BATCH-WITH-NO-ROWS",
        )
        result = pipeline.build_export("empty-export")
        assert result["ok"] is True
        assert result["empty_group"] is True
        export = result["export"]
        assert export["record_count"] == 0
        assert export["delivery_state"] == "confirmed"
        assert export["refresh_state"] == "completed"
        assert export["scope"]["requested_batches"] == ["BATCH-WITH-NO-ROWS"]
        assert export["source_record_count"] == 0
        lineage = store.rows("data_lineage")[-1]
        assert lineage["delivery_state"] == "confirmed"
        assert lineage["empty_group"] is True

    def test_export_rejects_unrefreshed_existing_view(self, pipeline):
        """视图存在但尚未刷新时，交付返回未确认，而不是空文件。"""
        aggs = store.rows("aggregation_view")
        aggs.append({
            "id": 999,
            "视图名称": "unrefreshed-view",
            "聚合维度": "level",
            "指标定义": "count(time)",
            "status": "已生效",
            "pending": True,
            "abnormal": False,
        })
        result = pipeline.build_export("unrefreshed-view")
        assert result["ok"] is False
        assert result["reason"] == "refresh_not_confirmed"
        assert result["receipt"]["refresh_state"] == "unrefreshed"

    def test_export_rejects_interrupted_refresh(self, pipeline, sample_raw_logs):
        """刷新中断时结果尚未确认，交付不得落空文件。"""
        self._prepare_validated_batch(pipeline, sample_raw_logs)
        pipeline.refresh_aggregation(
            "interrupted-view", "level", "time", "count",
            refresh_token="tok-int", fail_after=2,
        )

        result = pipeline.build_export("interrupted-view")
        assert result["ok"] is False
        assert result["reason"] == "refresh_not_confirmed"
        assert result["receipt"]["refresh_state"] == "partial"
        assert result["receipt"]["confirmed"] is False
        assert not [
            e for e in store.rows("data_export")
            if e.get("导出范围") == "interrupted-view"
        ]

    def test_interrupted_refresh_scope_distinguishes_batches(self, pipeline):
        """中断后可在已成功与待重试批次之间核对来源。"""
        pipeline.register_source("two-batch-src", "http", "default")
        pipeline.validate_source("two-batch-src")
        first = pipeline.start_ingest_batch(
            "two-batch-src", "2026Q1", [{"body": "t1|INFO|m1"}]
        )
        first_no = first["batch"]["批次编号"]
        pipeline.parse_batch(first_no)
        pipeline.validate_entities(first_no)
        second = pipeline.start_ingest_batch(
            "two-batch-src", "2026Q2", [{"body": "t2|WARN|m2"}]
        )
        second_no = second["batch"]["批次编号"]
        pipeline.parse_batch(second_no)
        pipeline.validate_entities(second_no)

        result = pipeline.refresh_aggregation(
            "two-batch-view", "level", "time", "count",
            refresh_token="tok-two", fail_after=1,
        )
        assert result["ok"] is False
        assert result["refresh_state"] == "partial"
        assert first_no in result["completed_batches"]
        assert second_no in result["pending_batches"]
        assert result["scope"]["record_count"] == 1

    def test_resume_same_token_keeps_completed_and_retries_pending(self, pipeline):
        """同一次刷新中断后带同一令牌恢复，只补算待重试部分。"""
        pipeline.register_source("resume-src", "http", "default")
        pipeline.validate_source("resume-src")
        logs = [
            {"body": "t1|INFO|m1"},
            {"body": "t2|WARN|m2"},
            {"body": "t3|INFO|m3"},
        ]
        batch = pipeline.start_ingest_batch("resume-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        interrupted = pipeline.refresh_aggregation(
            "resume-view", "level", "time", "count",
            refresh_token="tok-resume", fail_after=1,
        )
        assert interrupted["ok"] is False
        assert interrupted["refresh_state"] == "partial"
        assert interrupted["view"]["results"]["INFO"]["count"] == 1

        resumed = pipeline.refresh_aggregation(
            "resume-view", "level", "time", "count",
            refresh_token="tok-resume",
        )
        assert resumed["ok"] is True
        assert resumed["refresh_state"] == "completed"
        results = resumed["view"]["results"]
        assert results["INFO"]["count"] == 2
        assert results["WARN"]["count"] == 1

    def test_same_request_id_records_single_delivery(self, pipeline, sample_raw_logs):
        """同一申请编号重复提交，只记一次交付。"""
        self._prepare_validated_batch(pipeline, sample_raw_logs)
        pipeline.refresh_aggregation("dedup-delivery", "level", "time", "count")
        first = pipeline.build_export("dedup-delivery", request_id="req-001")
        second = pipeline.build_export("dedup-delivery", request_id="req-001")
        assert first["ok"] is True
        assert second["ok"] is True
        assert second["duplicate"] is True
        matched = [
            e for e in store.rows("data_export")
            if e.get("request_id") == "req-001"
        ]
        assert len(matched) == 1

    def test_legacy_empty_view_format_still_deliverable(self, pipeline):
        """兼容旧格式：已有 results 字段、无 refresh_state 的空视图可交付。"""
        store.rows("aggregation_view").append({
            "id": 1001,
            "视图名称": "legacy-empty",
            "聚合维度": "level",
            "指标定义": "count(time)",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": {},
        })
        receipt = pipeline.aggregation_receipt("legacy-empty")
        assert receipt["refresh_state"] == "completed"
        assert receipt["empty_group"] is True

        result = pipeline.build_export("legacy-empty")
        assert result["ok"] is True
        assert result["empty_group"] is True

    def test_concurrent_export_during_refresh_is_not_confirmed(self, pipeline):
        """第一次刷新与文件申请并发时，刷新中不得给出成功交付。"""
        from app.services import pipeline as pipeline_module

        aggs = store.rows("aggregation_view")
        aggs.append({
            "id": 2001,
            "视图名称": "refreshing-view",
            "聚合维度": "level",
            "指标定义": "count(time)",
            "status": "已生效",
            "pending": True,
            "abnormal": False,
            "refresh_state": pipeline_module.REFRESH_REFRESHING,
            "results": {},
        })
        result = pipeline.build_export("refreshing-view")
        assert result["ok"] is False
        assert result["reason"] == "refresh_not_confirmed"
        assert "尚未确认" in result["message"]

    def test_threaded_refresh_and_export_see_intermediate_state(self, pipeline, sample_raw_logs):
        """刷新与文件申请真正并发时，申请方看到的是刷新中、结果未确认。"""
        import threading

        self._prepare_validated_batch(pipeline, sample_raw_logs, source="thread-src")

        entered = threading.Event()
        release = threading.Event()
        original_aggregate = pipeline._aggregate

        def slow_aggregate(entities, dim_field, metric_field):
            entered.set()
            release.wait(timeout=5)
            return original_aggregate(entities, dim_field, metric_field)

        pipeline._aggregate = slow_aggregate
        outcome: dict = {}

        def do_refresh():
            outcome["refresh"] = pipeline.refresh_aggregation(
                "thread-view", "level", "time", "count"
            )

        worker = threading.Thread(target=do_refresh)
        try:
            worker.start()
            assert entered.wait(timeout=5), "刷新未进入取数阶段"
            # 刷新已进入取数阶段但尚未完成，此时申请交付。
            during = pipeline.build_export("thread-view")
            assert during["ok"] is False
            assert during["reason"] == "refresh_not_confirmed"
            assert during["receipt"]["refresh_state"] == "refreshing"
        finally:
            release.set()
            worker.join(timeout=5)
            pipeline._aggregate = original_aggregate

        assert outcome["refresh"]["ok"] is True
        after = pipeline.build_export("thread-view")
        assert after["ok"] is True
        assert after["receipt"]["refresh_state"] == "completed"

    def test_failed_refresh_without_progress_is_distinct(self, pipeline, sample_raw_logs):
        """刷新在建库阶段即失败时状态为 failed，区别于零行匹配的 completed。"""
        self._prepare_validated_batch(pipeline, sample_raw_logs, source="fail-src")
        result = pipeline.refresh_aggregation(
            "failed-view", "level", "time", "count",
            refresh_token="tok-fail", fail_after=0,
        )
        assert result["ok"] is False
        assert result["refresh_state"] == "failed"
        receipt = pipeline.aggregation_receipt("failed-view")
        assert receipt["confirmed"] is False
        assert receipt["empty_group"] is False
        delivery = pipeline.build_export("failed-view")
        assert delivery["ok"] is False
        assert delivery["reason"] == "refresh_not_confirmed"

    def test_completed_empty_and_completed_nonempty_are_distinguishable(
        self, pipeline, sample_raw_logs
    ):
        """空文件必须能和“完成但零行匹配”对上，而不是误判成未刷新。"""
        batch_no = self._prepare_validated_batch(
            pipeline, sample_raw_logs, source="dist-src"
        )
        pipeline.refresh_aggregation(
            "dist-empty", "level", "time", "count", source_batch="NO-SUCH"
        )
        pipeline.refresh_aggregation(
            "dist-data", "level", "time", "count", source_batch=batch_no
        )
        empty = pipeline.aggregation_receipt("dist-empty")
        data = pipeline.aggregation_receipt("dist-data")
        assert empty["refresh_state"] == data["refresh_state"] == "completed"
        assert empty["empty_group"] is True
        assert data["empty_group"] is False
        assert data["dimension_count"] > 0
        assert empty["scope"]["requested_batches"] == ["NO-SUCH"]
        assert data["scope"]["source_batches"] == [batch_no]
