"""List / Watch 目录接入编排。

边界约定（与业务侧三条关注一一对应）：

1. 一轮 List 由若干 scope 的分页组成。任何一页失败、403 之外的非 200、
   或同一快照各页 resourceVersion 不一致，整轮标记 failed，旧目录保持
   为当前视图，未完成的分页不会被宣布为全量。
2. Watch 干净断开时，凭最后一个 BOOKMARK / 事件携带的不透明
   resourceVersion 原样续接；收到 410 时不猜任何版本号，直接开始一轮
   有依据的重新 List。
3. 每轮有独立 round id，Watch 连接绑定 round。晋升新轮次时旧连接全部
   栅栏化，旧连接迟到事件只进入 ``staged_events`` 审计（stale=1），
   不写入当前对象视图。删除严格按 UID 命中，同名重建互不影响。

网络读取在后台线程进行，所有归档与目录写操作都在驱动线程内完成，
SQLite 连接不会跨线程共享。
"""

import json
import queue
import threading

from .catalog import COV_COVERED, COV_RESTRICTED, EVT_BOOKMARK
from .kubeclient import WatchDisconnected, WatchExpired

CLUSTER_SCOPE = "*"


class _WatchHandle:
    """一条后台 Watch 读取线程及其事件队列。"""

    def __init__(self, runner, connection_id, round_id):
        self.runner = runner
        self.connection_id = connection_id
        self.round_id = round_id
        self.closed = False

    def get(self, timeout=None):
        return self.runner.events.get(timeout=timeout)

    @property
    def url(self):
        return self.runner.stream.url

    def stop(self):
        if self.closed:
            return
        self.closed = True
        self.runner.stop()


class _WatchRunner:
    def __init__(self, stream):
        self.stream = stream
        self.events = queue.Queue()
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            while not self._stop.is_set():
                try:
                    event = next(self.stream)
                except WatchExpired as expired:
                    self.events.put(("expired", expired))
                    return
                except WatchDisconnected as disconnected:
                    self.events.put(("disconnected", disconnected))
                    return
                except StopIteration:
                    self.events.put(("disconnected", WatchDisconnected("stream closed")))
                    return
                self.events.put(("event", event))
        finally:
            self.stream.close()

    def stop(self):
        self._stop.set()
        self.stream.close()


class Ingestor:
    def __init__(self, client, archive, catalog, cluster, scopes=None):
        self.client = client
        self.archive = archive
        self.catalog = catalog
        self.cluster = cluster
        # ["*"] 表示集群级列举；否则为 namespace 列表
        self.scopes = scopes or [CLUSTER_SCOPE]
        self._watch = None

    # ------------------------------------------------------------------
    # 一轮完整的重新列举（启动、410 之后、定期重列举都走这里）
    # ------------------------------------------------------------------

    def run_round(self, pre_watch=True):
        catalog = self.catalog
        round_id = catalog.begin_round(self.cluster, note="list-round")
        resume_token = catalog.resume_token(self.cluster)
        watch = None

        if pre_watch and resume_token:
            # 先把 Watch 挂在旧检查点上，分页期间到达的事件进入暂存区。
            try:
                watch = self._start_watch(round_id, resume_token)
            except WatchExpired:
                # 旧检查点已被压缩：本轮作废，以全新 List 为唯一依据，
                # 绝不构造“更大”的版本号继续。
                catalog.fail_round(round_id, "resume token expired before list")
                round_id = catalog.begin_round(self.cluster, note="relist-after-410")
                watch = None

        try:
            snapshot_rv, present_keys = self._list_all_scopes(round_id, catalog)
        except Exception:
            catalog.fail_round(round_id, "unexpected list failure")
            if watch is not None:
                watch.stop()
            raise
        if snapshot_rv is None:
            catalog.fail_round(round_id, "no fully covered scope")
            if watch is not None:
                watch.stop()
            return _RoundResult(round_id, False, reason="no covered scope")

        try:
            if watch is not None:
                # 分页交接：把分页期间暂存的事件按到达顺序回放到新目录，
                # (uid, rv) 相等即去重，不做任何版本大小比较。
                self._drain_into_staging(watch, round_id)
            self._replay_staged(round_id, watch)
            # 重列举之后，权威续接点是新 List 快照的不透明 RV；旧 kv 中的
            # 检查点可能已被压缩，绝不能继续使用。重看造成的重叠由 (uid,rv)
            # 相等去重吸收。
            catalog.promote_round(round_id, self.cluster, snapshot_rv,
                                  present_keys, snapshot_rv)
        except (WatchExpired, WatchDisconnected) as handoff_failure:
            # 分页交接期间 Watch 失效（410 或断连）：事件交接不完整，
            # 整轮作废，绝不让部分分页晋升为全量。
            catalog.fail_round(round_id, "watch lost during pagination handoff")
            if watch is not None:
                watch.stop()
            raise handoff_failure
        if watch is not None:
            # 晋升完成，暂存连接转为当前轮次的常驻连接。
            self._attach(watch)
        return _RoundResult(
            round_id, True, snapshot_rv=snapshot_rv,
            covered_scopes=[
                row["scope"] for row in catalog.coverage(self.cluster)
                if row["state"] == COV_COVERED and row["round_id"] == round_id
            ],
            connection_id=None if watch is None else watch.connection_id,
        )

    def _list_all_scopes(self, round_id, catalog):
        snapshot_rv, present_keys, covered = None, [], []
        for scope in self.scopes:
            namespace = None if scope == CLUSTER_SCOPE else scope
            page_index, continue_token, scope_rvs = 0, "", set()
            while True:
                page = self.client.list_page(
                    self.archive, self.cluster, namespace, continue_token
                )
                if page.status == 403:
                    catalog.record_page(round_id, self.cluster, scope, page_index,
                                        continue_token, page.resource_version,
                                        page.status, page.capture_id)
                    # 受限是显式范围证据：保留旧对象，不做删除推断。
                    catalog.record_coverage(self.cluster, scope, round_id,
                                            COV_RESTRICTED, page.capture_id)
                    break
                if page.status != 200:
                    catalog.fail_round(round_id, f"list status {page.status} on {scope}")
                    return None, present_keys
                if not page.resource_version:
                    catalog.fail_round(round_id, "list page without resourceVersion")
                    return None, present_keys
                scope_rvs.add(page.resource_version)
                if len(scope_rvs) > 1:
                    catalog.fail_round(round_id, "inconsistent snapshot resourceVersion")
                    return None, present_keys
                page_id = catalog.record_page(
                    round_id, self.cluster, scope, page_index, continue_token,
                    page.resource_version, page.status, page.capture_id,
                )
                for obj in page.payload.get("items", []):
                    meta = obj["metadata"]
                    catalog.upsert_list_object(round_id, self.cluster, scope, obj, page_id)
                    present_keys.append((meta["uid"], scope))
                if not page.continue_token:
                    catalog.record_coverage(self.cluster, scope, round_id,
                                            COV_COVERED, page.capture_id)
                    covered.append(scope)
                    break
                page_index += 1
                continue_token = page.continue_token
            if scope_rvs and snapshot_rv is None:
                snapshot_rv = next(iter(scope_rvs))
        return snapshot_rv if covered else None, present_keys

    def _replay_staged(self, round_id, watch):
        connection_id = None if watch is None else watch.connection_id
        rows = self.catalog.connection.execute(
            "SELECT * FROM staged_events WHERE round_id=? ORDER BY id", (round_id,)
        ).fetchall()
        for row in rows:
            if row["stale"] or row["applied"]:
                continue
            if connection_id is not None and row["connection_id"] != connection_id:
                continue
            event = json.loads(row["payload"])
            # 分页交接回放不推进 resume token：晋升时以新 List 快照 RV 为
            # 唯一权威续接点，避免暂存区里的旧 token 覆盖它。
            self.catalog.apply_watch_event(
                round_id, self.cluster, row["connection_id"], row["id"],
                event, row["capture_id"], advance_resume=False,
            )

    # ------------------------------------------------------------------
    # Watch 常驻：续接 vs 410 重列举
    # ------------------------------------------------------------------

    def start(self):
        """确保已有当前目录并挂上常驻 Watch。"""
        if self.catalog.active_round(self.cluster) is None:
            self.run_round(pre_watch=False)
        self._open_resident_watch()

    def _open_resident_watch(self):
        if self._watch is not None and not self._watch.closed:
            return self._watch
        token = self.catalog.resume_token(self.cluster) or ""
        round_id = self.catalog.active_round(self.cluster)
        try:
            self._watch = self._start_watch(round_id, token)
        except WatchExpired:
            # 续接点已被压缩：以全新 List 重建有依据的目录，再挂 Watch。
            # 不重试同一 token，更不构造“更大”的版本号。
            result = self.run_round(pre_watch=False)
            try:
                self._watch = self._start_watch(
                    result.round_id, self.catalog.resume_token(self.cluster) or ""
                )
            except WatchExpired as still_expired:
                # 全新快照仍被拒，说明服务端状态异常：交回调用方，不留假视图。
                self._watch = None
                raise still_expired
        return self._watch

    def pump(self, timeout=0.2):
        """处理当前 Watch 队列中已到达的消息，返回处理条数。

        410 触发整轮重列举并重新挂 Watch；干净断连凭不透明 token 原样续接。
        """
        watch = self._open_resident_watch()
        processed = 0
        while True:
            try:
                kind, payload = watch.get(timeout=0 if processed else timeout)
            except queue.Empty:
                return processed
            if kind == "event":
                capture_id = self._archive_event(payload, watch)
                self._ingest_event(watch, payload, capture_id)
                processed += 1
                continue
            self.catalog.close_connection(
                watch.connection_id,
                "410 expired" if kind == "expired" else f"disconnected: {payload}",
            )
            watch.stop()
            self._watch = None
            if kind == "expired":
                # 有依据的目录重建：全新 List，不猜版本号。
                self.run_round(pre_watch=False)
            watch = self._open_resident_watch()
            processed += 1

    def drain(self, timeout=0.5):
        """排空队列直到一个空闲窗口，返回处理条数（测试/演示用）。"""
        total = 0
        while True:
            processed = self.pump(timeout=timeout)
            total += processed
            if processed == 0:
                return total

    def stop(self):
        if self._watch is not None:
            self.catalog.close_connection(self._watch.connection_id, "stopped")
            self._watch.stop()
            self._watch = None

    def _archive_event(self, event, watch):
        """在驱动线程内把事件原文追加到只追加归档，返回 capture 编号。"""
        return self.archive.append(
            self.cluster, watch.url, 200,
            {"content-type": "application/json; stream=watch"},
            (json.dumps(event, ensure_ascii=False)).encode("utf-8"),
        )

    def _ingest_event(self, watch, event, capture_id):
        catalog = self.catalog
        staged_id = catalog.stage_event(
            watch.round_id, self.cluster, watch.connection_id, event, capture_id
        )
        if not catalog.is_connection_active(watch.connection_id):
            # 旧连接回流：只留审计证据，绝不碰当前视图。
            catalog.mark_stale(staged_id)
            return
        if event["type"] == EVT_BOOKMARK:
            catalog.apply_watch_event(
                watch.round_id, self.cluster, watch.connection_id,
                staged_id, event, capture_id,
            )
            return
        meta = event.get("object", {}).get("metadata", {})
        uid, rv = meta.get("uid"), str(meta.get("resourceVersion", ""))
        if uid is not None and catalog.already_seen(self.cluster, uid, rv):
            # 与 List 快照或先前事件同版本：只标记，不重复应用。
            with catalog.transaction() as conn:
                conn.execute("UPDATE staged_events SET applied=1 WHERE id=?", (staged_id,))
            return
        catalog.apply_watch_event(
            watch.round_id, self.cluster, watch.connection_id,
            staged_id, event, capture_id,
        )

    def _start_watch(self, round_id, resource_version):
        # 集群级（["*"]）或多 namespace 采集走集群级 watch；单 namespace
        # 采集只 watch 该 namespace。
        if len(self.scopes) == 1 and self.scopes[0] != CLUSTER_SCOPE:
            namespace, scope = self.scopes[0], self.scopes[0]
        else:
            namespace, scope = None, CLUSTER_SCOPE
        stream = self.client.open_watch(
            self.archive, self.cluster, namespace, resource_version
        )
        connection_id = self.catalog.open_connection(
            round_id, self.cluster, scope, resource_version
        )
        runner = _WatchRunner(stream)
        handle = _WatchHandle(runner, connection_id, round_id)
        # 消费服务端连接建立时给出的 BOOKMARK，作为第一条进度证据。
        kind, payload = handle.get(timeout=10)
        if kind == "event":
            event = payload
            capture_id = self._archive_event(event, handle)
            if event.get("type") == EVT_BOOKMARK:
                staged_id = self.catalog.stage_event(
                    round_id, self.cluster, connection_id, event, capture_id
                )
                self.catalog.apply_watch_event(
                    round_id, self.cluster, connection_id, staged_id, event, capture_id
                )
        elif kind == "expired":
            handle.stop()
            raise payload
        else:
            handle.stop()
            raise WatchDisconnected(str(payload))
        return handle

    def _attach(self, watch):
        if self._watch is not None and self._watch is not watch:
            self._watch.stop()
        self._watch = watch

    def _drain_into_staging(self, watch, round_id):
        """把分页期间后台线程收到的消息全部搬进暂存表。"""
        while True:
            try:
                kind, payload = watch.get(timeout=0.05)
            except queue.Empty:
                return
            if kind != "event":
                # 分页期间 watch 已失效：事件交接不完整，交由调用方失败处理。
                watch.stop()
                if kind == "expired":
                    raise payload
                raise WatchDisconnected(str(payload))
            event = payload
            capture_id = self._archive_event(event, watch)
            self.catalog.stage_event(
                round_id, self.cluster, watch.connection_id, event, capture_id
            )


class _RoundResult:
    def __init__(self, round_id, promoted, reason=None, snapshot_rv=None,
                 covered_scopes=None, connection_id=None):
        self.round_id = round_id
        self.promoted = promoted
        self.reason = reason
        self.snapshot_rv = snapshot_rv
        self.covered_scopes = covered_scopes or []
        self.connection_id = connection_id

    def as_dict(self):
        return {
            "round_id": self.round_id,
            "promoted": self.promoted,
            "reason": self.reason,
            "snapshot_rv": self.snapshot_rv,
            "covered_scopes": self.covered_scopes,
            "connection_id": self.connection_id,
        }
