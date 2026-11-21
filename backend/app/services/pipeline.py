"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from typing import Any

from app.store import store

# 聚合刷新与交付申请共享同一份视图状态：刷新可能先于交付申请创建视图，
# 也可能在交付申请读取时仍处于刷新中，因此用同一把可重入锁串行化状态写入。
_DELIVERY_LOCK = threading.RLock()

REFRESH_COMPLETED = "completed"
REFRESH_REFRESHING = "refreshing"
REFRESH_PARTIAL = "partial"
REFRESH_FAILED = "failed"
REFRESH_NOT_STARTED = "unrefreshed"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def _select_agg_source(self, source_batch: str | None) -> list[dict[str, Any]]:
        """选取参与聚合的标准化明细，并记录实际取数范围。

        只有“校验通过且字段集可聚合”的记录才进入取数范围：无法解析的字段集
        既不会贡献分组，也不应占用刷新进度，否则中断边界的“已成功/待重试”
        会与真正的数据行对不上。
        """
        parsed = store.rows("parsed_entity")
        selected: list[dict[str, Any]] = []
        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            if source_batch and entity.get("来源批次") != source_batch:
                continue
            try:
                json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue
            selected.append(entity)
        return selected

    def _aggregate(
        self,
        entities: list[dict[str, Any]],
        dim_field: str,
        metric_field: str,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for entity in entities:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)
        return results

    def _scope_snapshot(
        self,
        entities: list[dict[str, Any]],
        source_batch: str | None,
        completed_batches: list[str] | None = None,
        pending_batches: list[str] | None = None,
        requested_batches: list[str] | None = None,
    ) -> dict[str, Any]:
        """记录本次取数范围，供交付清单与空文件核对来源。

        `source_batches` 是实际取到数据的批次；`requested_batches` 是本次
        申请取数的批次范围。零分组时前者为空、后者仍保留，空文件因此有明确来源。
        """
        observed = sorted({
            str(entity.get("来源批次", ""))
            for entity in entities
            if entity.get("来源批次")
        })
        if requested_batches is None:
            # 未限定批次时，本次申请覆盖的就是实际取到的全部批次；
            # 限定了批次但零行匹配时也保留该批次，空文件仍有明确来源。
            requested = [source_batch] if source_batch else list(observed)
        else:
            requested = list(requested_batches)
        return {
            "source_batch": source_batch or "",
            "source_batches": observed,
            "requested_batches": requested,
            "completed_batches": list(completed_batches or []),
            "pending_batches": list(pending_batches or []),
            "record_count": len(entities),
            "metric_filter": "status=校验通过",
        }

    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        source_batch: str | None = None,
        refresh_token: str | None = None,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。

        刷新分两个阶段落库：开始时写入 `refresh_state=refreshing`，
        完成/失败后写入最终状态。交付申请可以据此区分“未生成”、
        “刷新中/刷新失败”和“有效的空分组数据回执”。

        `refresh_token` 标识同一次刷新请求。若上次以该令牌中断，
        再次携带同一令牌会从已完成的取数边界续跑，并保留已成功分组。
        `fail_after` 用于模拟中断：本次处理该数量的明细后中断。
        """
        token = refresh_token or _stable_digest(
            {"view": view_name, "at": _now_iso(), "pid": time.time_ns()}
        )

        with _DELIVERY_LOCK:
            aggs = store.rows("aggregation_view")
            agg_entry = None
            for candidate in aggs:
                if candidate.get("视图名称") == view_name:
                    agg_entry = candidate
                    break
            if agg_entry is None:
                agg_entry = {
                    "id": _next_id("aggregation_view"),
                    "视图名称": view_name,
                    "聚合维度": dim_field,
                    "指标定义": f"{agg_func}({metric_field})",
                    "数据源范围": "标准化明细",
                    "刷新间隔": "manual",
                    "status": "已生效",
                    "pending": True,
                    "abnormal": False,
                    "results": {},
                    "dimensions": 0,
                }
                aggs.append(agg_entry)

            # 只有同一刷新请求（同令牌）且上次未完成时，才从断点续跑。
            previous_scope = agg_entry.get("refresh_scope") or {}
            resumable = (
                previous_scope.get("refresh_token") == token
                and agg_entry.get("refresh_state") in (REFRESH_PARTIAL, REFRESH_FAILED)
                and int(previous_scope.get("progress", 0)) > 0
            )
            resume_results = (
                copy.deepcopy(agg_entry.get("results") or {}) if resumable else {}
            )
            resume_from = int(previous_scope.get("progress", 0)) if resumable else 0

            agg_entry.update({
                "聚合维度": dim_field,
                "指标定义": f"{agg_func}({metric_field})",
                "status": "已生效",
                "refresh_state": REFRESH_REFRESHING,
                "pending": True,
                "abnormal": False,
                "refresh_token": token,
                "refresh_started_at": _now_iso(),
                "refresh_error": "",
                "results": resume_results,
                "dimensions": len(resume_results),
                "progress": resume_from,
            })

        # 取数发生在锁外：此时视图已标记为 refreshing，交付申请会看到
        # “刷新中”而不是把它误判成未生成或一行未匹配。
        entities = self._select_agg_source(source_batch)
        scope = self._scope_snapshot(entities, source_batch)
        batches_in_order = self._ordered_batches(entities)

        # 本次尝试处理的明细区间；中断时该区间完成的部分单独固化。
        process_from = resume_from
        process_to = (
            len(entities) if fail_after is None
            else min(resume_from + max(fail_after, 0), len(entities))
        )
        attempt = entities[process_from:process_to]

        # 一个批次只有在其全部记录都已被取数后，才算已成功完成；
        # 中断时其余批次留在 pending，供恢复时核对来源。
        completed_batches = self._fully_covered_batches(entities, process_to)
        pending_batches = [b for b in batches_in_order if b not in completed_batches]

        try:
            partial = self._aggregate(attempt, dim_field, metric_field)
            results = self._merge_results(resume_results, partial)
            if fail_after is not None:
                raise RuntimeError("模拟刷新中断")
        except Exception as exc:  # noqa: BLE001 - 需要把失败固化到视图状态
            progress = process_to
            with _DELIVERY_LOCK:
                # 中断时把“已成功取到的分组”和“仍需重试的取数边界”一起固化，
                # 结果尚未整体确认，但重建后可逐批核对来源。
                agg_entry.update({
                    "refresh_state": REFRESH_PARTIAL if results else REFRESH_FAILED,
                    "pending": True,
                    "abnormal": True,
                    "refresh_error": str(exc),
                    "refresh_finished_at": _now_iso(),
                    "results": results,
                    "dimensions": len(results),
                    "progress": progress,
                    "refresh_scope": {
                        "refresh_token": token,
                        "progress": progress,
                        "completed_batches": completed_batches,
                        "pending_batches": pending_batches,
                    },
                    "scope": self._scope_snapshot(
                        entities[:process_to], source_batch,
                        completed_batches, pending_batches,
                    ),
                })
                return {
                    "ok": False,
                    "message": f"聚合视图 {view_name} 刷新中断：{exc}",
                    "view": agg_entry,
                    "refresh_token": token,
                    "refresh_state": agg_entry["refresh_state"],
                    "dimensions": len(results),
                    "completed_batches": completed_batches,
                    "pending_batches": pending_batches,
                    "scope": agg_entry["scope"],
                }

        with _DELIVERY_LOCK:
            agg_entry.update({
                "results": results,
                "dimensions": len(results),
                "refresh_state": REFRESH_COMPLETED,
                "pending": False,
                "abnormal": False,
                "refresh_error": "",
                "refresh_token": token,
                "refreshed_at": _now_iso(),
                "refresh_finished_at": _now_iso(),
                "source_records": scope["record_count"],
                "scope": self._scope_snapshot(
                    entities, source_batch, batches_in_order, []
                ),
                "refresh_scope": {
                    "refresh_token": token,
                    "progress": len(entities),
                    "completed_batches": batches_in_order,
                    "pending_batches": [],
                },
                "progress": len(entities),
            })

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "refresh_token": token,
            "refresh_state": REFRESH_COMPLETED,
            "dimensions": len(results),
            "scope": scope,
        }

    @staticmethod
    def _ordered_batches(entities: list[dict[str, Any]]) -> list[str]:
        ordered: list[str] = []
        for entity in entities:
            batch = str(entity.get("来源批次", ""))
            if batch and batch not in ordered:
                ordered.append(batch)
        return ordered

    @classmethod
    def _fully_covered_batches(cls, entities: list[dict[str, Any]], upto: int) -> list[str]:
        """取数前 `upto` 条内，所有记录都已被取到的批次。"""
        totals: dict[str, int] = {}
        for entity in entities:
            batch = str(entity.get("来源批次", ""))
            if batch:
                totals[batch] = totals.get(batch, 0) + 1
        seen: dict[str, int] = {}
        for entity in entities[:upto]:
            batch = str(entity.get("来源批次", ""))
            if batch:
                seen[batch] = seen.get(batch, 0) + 1
        return [
            batch for batch in cls._ordered_batches(entities)
            if seen.get(batch, 0) >= totals.get(batch, 0)
        ]

    @staticmethod
    def _merge_results(
        base: dict[str, dict[str, Any]],
        delta: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """把续跑新增的分组合并进已成功结果，避免覆盖已核对部分。"""
        merged = copy.deepcopy(base)
        for dim, bucket in delta.items():
            target = merged.setdefault(dim, {"count": 0, "sum": 0.0, "values": []})
            target["count"] += bucket["count"]
            target["sum"] += bucket["sum"]
            target["values"].extend(bucket["values"])
        return merged

    def aggregation_receipt(self, view_name: str) -> dict[str, Any] | None:
        """返回聚合视图当前刷新状态的结构化回执；视图从未生成时返回 None。

        回执把三种“看起来没有数据”的情况分开：
        - 视图不存在：调用方得到 None（未生成）；
        - 视图存在但刷新中/刷新失败：refresh_state 明确标出，结果尚未确认；
        - 视图存在且刷新完成但没有任何分组：refresh_state=completed，results={}，
          这是有效的空分组数据回执，`empty_group=True`。
        兼容旧格式：只有 `results` 字段、没有 refresh_state 的历史视图按完成处理。
        """
        for agg in store.rows("aggregation_view"):
            if agg.get("视图名称") != view_name:
                continue
            state = agg.get("refresh_state")
            if state is None:
                # 兼容已存在的空视图交付格式：有 results 视为完成，否则视为未刷新。
                state = REFRESH_COMPLETED if "results" in agg else REFRESH_NOT_STARTED
            results = agg.get("results") or {}
            scope = agg.get("scope")
            if scope is None:
                scope = {
                    "source_batch": "",
                    "source_batches": sorted({
                        str(entity.get("来源批次", ""))
                        for entity in store.rows("parsed_entity")
                        if entity.get("来源批次") and entity.get("status") == "校验通过"
                    }) if "results" in agg else [],
                    "requested_batches": [],
                    "completed_batches": [],
                    "pending_batches": [],
                    "record_count": agg.get("source_records", len(results)),
                    "metric_filter": "status=校验通过",
                }
            completed = list(scope.get("completed_batches") or [])
            pending = list(scope.get("pending_batches") or [])
            confirmed = state == REFRESH_COMPLETED
            return {
                "view_name": view_name,
                "refresh_state": state,
                "confirmed": confirmed,
                "empty_group": confirmed and not results,
                "refresh_token": agg.get("refresh_token", ""),
                "refreshed_at": agg.get("refreshed_at", ""),
                "refresh_error": agg.get("refresh_error", ""),
                "dimension_count": len(results),
                "record_count": scope.get("record_count"),
                "scope": scope,
                "completed_batches": completed,
                "pending_batches": pending,
                "stats": {
                    "dimensions": len(results),
                    "records": scope.get("record_count"),
                    "completed_batches": len(completed),
                    "pending_batches": len(pending),
                },
                "results": results,
            }
        return None

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """从聚合视图生成交付清单与导出文件。

        只有存在聚合并处于 `completed`（有效空分组也算完成）的视图才交付成功；
        未生成、刷新中、刷新失败都返回 `ok=False` 并给出可核对的状态回执，
        不会落一份和刷新状态对不上的空文件。
        """
        with _DELIVERY_LOCK:
            # 首次读取与落盘之间视图可能被刷新/中断，统一在锁内读取一次，
            # 以最新状态作为交付结论的唯一来源。
            receipt = self.aggregation_receipt(view_name)
            if receipt is None:
                return {
                    "ok": False,
                    "message": f"聚合视图 {view_name} 尚未生成，无法交付",
                    "reason": "view_not_found",
                    "receipt": None,
                }

            state = receipt["refresh_state"]
            if state in (
                REFRESH_NOT_STARTED, REFRESH_REFRESHING, REFRESH_FAILED, REFRESH_PARTIAL,
            ):
                return {
                    "ok": False,
                    "message": {
                        REFRESH_NOT_STARTED: f"聚合视图 {view_name} 尚未刷新，无法交付",
                        REFRESH_REFRESHING: f"聚合视图 {view_name} 正在刷新，结果尚未确认",
                        REFRESH_FAILED: f"聚合视图 {view_name} 刷新失败，无法交付",
                        REFRESH_PARTIAL: f"聚合视图 {view_name} 刷新未完成，结果尚未确认",
                    }[state],
                    "reason": "refresh_not_confirmed",
                    "receipt": receipt,
                    "progress": len(receipt["scope"].get("completed_batches", [])),
                    "completed_batches": receipt["scope"].get("completed_batches", []),
                    "pending_batches": receipt["scope"].get("pending_batches", []),
                    "source_batches": receipt["scope"].get("source_batches", []),
                }

            results_data = receipt["results"]
            scope = receipt["scope"]
            # “有效的空分组数据回执”：视图已完成刷新，但没有可交付分组。
            # 这是完成但零行匹配，可明确区别于“结果尚未确认”。
            empty_group = len(results_data) == 0

            exports = store.rows("data_export")
            if request_id:
                for existing in exports:
                    if existing.get("request_id") == request_id:
                        return {
                            "ok": True,
                            "message": f"交付申请 {request_id} 已记录，本次不重复交付",
                            "export": existing,
                            "receipt": receipt,
                            "duplicate": True,
                            "delivery_state": existing.get("delivery_state", "confirmed"),
                            "empty_group": bool(existing.get("empty_group")),
                        }

            export_id = _next_id("data_export")
            export_entry = {
                "id": export_id,
                "导出范围": view_name,
                "目标格式": export_format,
                "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
                "导出状态": "已完成",
                "文件路径": f"/exports/{view_name}.{export_format}",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "results": results_data,
                "record_count": len(results_data),
                "dimension_count": len(results_data),
                "empty_group": empty_group,
                "delivery_state": "confirmed",
                "delivery_conclusion": (
                    "已完成，空分组（已核对取数范围）"
                    if empty_group
                    else f"已完成，交付 {len(results_data)} 个分组"
                ),
                "refresh_state": receipt["refresh_state"],
                "refresh_token": receipt["refresh_token"],
                "view_refresh_token": receipt["refresh_token"],
                "refresh_stats": receipt["stats"],
                "scope": scope,
                "source_batches": scope.get("source_batches", []),
                "source_record_count": scope.get("record_count"),
                "request_id": request_id or "",
                "exported_at": _now_iso(),
            }
            exports.append(export_entry)

            lineages = store.rows("data_lineage")
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": view_name,
                "解析规则版本": receipt.get("refreshed_at", "unknown"),
                "标准化版本": view_name,
                "下游消费标识": f"export-{export_id}",
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "export_id": str(export_id),
                "export_format": export_format,
                "delivery_state": "confirmed",
                "refresh_token": receipt["refresh_token"],
                "refresh_stats": receipt["stats"],
                "source_batches": scope.get("source_batches", []),
                "record_count": len(results_data),
                "empty_group": empty_group,
            }
            lineages.append(lineage_entry)

        message = (
            f"数据交付 {view_name}.{export_format} 完成（空分组，来源已核对）"
            if empty_group
            else f"数据交付 {view_name}.{export_format} 完成"
        )
        return {
            "ok": True,
            "message": message,
            "export": export_entry,
            "lineage": lineage_entry,
            "receipt": receipt,
            "empty_group": empty_group,
            "duplicate": False,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()