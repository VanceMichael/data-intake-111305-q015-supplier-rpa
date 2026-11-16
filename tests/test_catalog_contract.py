"""List/Watch 目录接入的领域契约测试。

对应 fixtures/business_cases.json 的三个业务样例，并补齐分页交接、
不透明 RV、BOOKMARK、重启恢复、只读约束与证据查询等不变量。
全部使用本地 API 模拟器，不连接外部集群。
"""

import json
import tempfile
import time
import unittest
from pathlib import Path

from supplier_rpa.archive import Archive
from supplier_rpa.catalog import Catalog, Ingestor
from supplier_rpa.kube_client import KubeClient
from supplier_rpa.kube_sim import KubeStore, serve

SCOPES = ["cluster", "namespace:ops", "namespace:restricted"]


def wait_until(predicate, timeout=3.0, interval=0.01):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError("condition not met before timeout; last=%r" % (last,))


def make_ingestor(simulator, db_path, archive_path=None, scopes=None, page_limit=2):
    archive = Archive(archive_path) if archive_path else None
    catalog = Catalog(db_path)
    ingestor = Ingestor(KubeClient(simulator.base_url, read_timeout=0.05), catalog, "managed-a",
                        scopes or SCOPES, archive=archive, source="kubesim:managed-a",
                        page_limit=page_limit)
    return catalog, ingestor, archive


def pump(ingestor, times=40, interval=0):
    for _ in range(times):
        ingestor.pump_once()
        if interval:
            time.sleep(interval)


class CatalogContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = KubeStore()
        self.store.seed([
            {"metadata": {"uid": "uid-old", "name": "worker", "namespace": "lab", "resourceVersion": "rv-seed-1"}},
            {"metadata": {"uid": "uid-side", "name": "sidecar", "namespace": "lab", "resourceVersion": "rv-seed-2"}},
            {"metadata": {"uid": "uid-ops", "name": "batch", "namespace": "ops", "resourceVersion": "rv-seed-3"}},
            {"metadata": {"uid": "uid-denied", "name": "locked", "namespace": "restricted",
                          "resourceVersion": "rv-seed-4"}},
        ])
        self.store.set_forbidden("restricted", True)
        self.server_cm = serve(self.store)
        self.sim = self.server_cm.__enter__()
        self.catalog, self.ingestor, self.archive = make_ingestor(
            self.sim, self.root / "catalog.sqlite3", self.root / "raw.sqlite3")

    def tearDown(self):
        self.ingestor.shutdown()
        self.catalog.close()
        if self.archive is not None:
            self.archive.close()
        self.server_cm.__exit__(None, None, None)
        self.temporary.cleanup()

    def _alive_uids(self):
        return {row["uid"] for row in self.catalog.objects("managed-a")}

    def _row(self, uid):
        return self.catalog.object_provenance("managed-a", uid)["current"]

    def test_list_pagination_promotes_only_when_all_pages_complete(self):
        round_id = self.ingestor.synchronize("initial")
        state = self.catalog.round_state(round_id)
        self.assertEqual(state["state"], "promoted")
        pages = self.catalog.round_evidence(round_id)["list_pages"]
        # 集群级共 3 个可见对象（受限命名空间被过滤），page_limit=2 时需要两页。
        cluster_pages = [p for p in pages if p["scope"] == "cluster"]
        self.assertEqual(len(cluster_pages), 2)
        snapshot_rvs = {p["snapshot_rv"] for p in cluster_pages}
        self.assertEqual(len(snapshot_rvs), 1)  # 同一轮分页必须共享同一个快照 RV
        self.assertTrue(all(p["http_status"] == 200 for p in cluster_pages))
        self.assertEqual(self._alive_uids(), {"uid-old", "uid-side", "uid-ops"})

    def test_failed_pagination_abandons_round_and_keeps_previous_view(self):
        first = self.ingestor.synchronize("initial")
        self.sim.admin("flags", {"fail_continue": True})
        with self.assertRaises(RuntimeError):
            self.ingestor.synchronize("periodic")
        rows = self.catalog.connection.execute(
            "SELECT round_id,state FROM rounds ORDER BY started_at").fetchall()
        states = {row["round_id"]: row["state"] for row in rows}
        self.assertEqual(states[first], "promoted")
        newer = [rid for rid in states if rid != first]
        self.assertTrue(newer)
        self.assertTrue(all(states[rid] == "abandoned" for rid in newer))
        # 半成品轮次不能被宣布为全量：当前活动轮次仍是首轮。
        self.assertEqual(self.catalog.active_round("managed-a")["round_id"], first)
        self.assertEqual(self._alive_uids(), {"uid-old", "uid-side", "uid-ops"})

    def test_paged_snapshot_rv_mismatch_is_not_stitched_into_a_catalog(self):
        self.ingestor.synchronize("initial")
        self.sim.admin("flags", {"rv_mismatch": True})
        with self.assertRaisesRegex(RuntimeError, "resourceVersion mismatch"):
            self.ingestor.synchronize("periodic")
        active = self.catalog.active_round("managed-a")
        evidence = self.catalog.round_evidence(active["round_id"])
        self.assertTrue(all(p["http_status"] == 200 for p in evidence["list_pages"]))

    def test_events_during_pagination_are_covered_by_watch_handoff(self):
        # 单范围（cluster）下：第一页快照固定后、第二页请求前注入变更。
        # 该变更不进入本轮 List，而由 Watch 从首页快照 RV 回放补齐。
        catalog = Catalog(self.root / "handoff.sqlite3")
        ingestor = Ingestor(KubeClient(self.sim.base_url, read_timeout=0.05), catalog,
                            "managed-a", ["cluster"], page_limit=2)
        try:
            def mutate_after_first_page(namespace, page_index):
                if namespace == "" and page_index == ingestor.page_limit:
                    self.sim.admin("mutate", {"op": "put", "object": {
                        "metadata": {"uid": "uid-mid", "name": "midflight", "namespace": "ops"}}})
                    self.store.page_callbacks.remove(callback)

            callback = mutate_after_first_page
            self.store.page_callbacks.append(callback)
            round_id = ingestor.synchronize("initial")
            self.assertNotIn("uid-mid", {r["uid"] for r in catalog.objects("managed-a")})
            pump(ingestor)
            row = catalog.object_provenance("managed-a", "uid-mid")["current"]
            self.assertEqual(row["origin_kind"], "watch")
            self.assertEqual(row["origin_round_id"], round_id)
        finally:
            ingestor.shutdown()
            catalog.close()

    def test_uid_recreation_survives_late_delete_of_the_old_lifecycle(self):
        self.ingestor.synchronize("initial")
        pump(self.ingestor)
        self.sim.admin("mutate", {"op": "delete", "namespace": "lab", "name": "worker"})
        self.sim.admin("mutate", {"op": "put", "object": {
            "metadata": {"uid": "uid-new", "name": "worker", "namespace": "lab"}}})
        pump(self.ingestor)
        old_row = self._row("uid-old")
        new_row = self._row("uid-new")
        self.assertEqual((old_row["alive"], old_row["name"]), (0, "worker"))
        self.assertEqual((new_row["alive"], new_row["name"]), (1, "worker"))
        # 旧生命周期迟到的删除事件（相同 name、旧 UID）不能抹掉新对象。
        self.sim.admin("mutate", {"op": "put", "object": {
            "metadata": {"uid": "uid-late", "name": "ghost", "namespace": "lab"}}})
        pump(self.ingestor)
        self.assertIn("uid-late", self._alive_uids())
        late_event = {"type": "DELETED", "object": {"metadata": {
            "uid": "uid-old", "name": "worker", "namespace": "lab", "resourceVersion": "rv-late-1"}}}
        # 通过正常 Watch 路径回放一条旧 UID 的迟到删除。
        conn_id = self.ingestor.streams["cluster"].conn_id
        self.sim.admin("inject", {"conn_id": conn_id, "event": late_event})
        pump(self.ingestor)
        self.assertEqual(self._row("uid-new")["alive"], 1)
        self.assertEqual(self._row("uid-old")["alive"], 0)

    def test_stale_round_connection_events_are_fenced_with_touching_view(self):
        first = self.ingestor.synchronize("initial")
        pump(self.ingestor)
        # 让集群级 Watch 410，触发重列举；ops 级旧连接跨越轮次保留。
        self.sim.admin("expire", {"conn_id": self.ingestor.streams["cluster"].conn_id})
        pump(self.ingestor, times=10)
        self.assertTrue(self.ingestor.relist_pending)
        second = self.ingestor.synchronize(self.ingestor.relist_reason)
        self.assertNotEqual(first, second)
        pump(self.ingestor, times=30)
        stale = self.catalog.connection.execute(
            "SELECT conn_id FROM connections WHERE round_id=? AND state='superseded'", (first,)).fetchall()
        self.assertTrue(stale)
        self.sim.admin("inject", {"conn_id": stale[0]["conn_id"], "event": {
            "type": "DELETED", "object": {"metadata": {
                "uid": "uid-ops", "name": "batch", "namespace": "ops", "resourceVersion": "rv-stale-9"}}}})
        pump(self.ingestor, times=30)
        self.assertEqual(self._row("uid-ops")["alive"], 1)
        stats = self.catalog.fence_stats("managed-a")
        self.assertEqual(stats.get("stale_round_connection"), 1)
        fenced = self.catalog.connection.execute(
            "SELECT * FROM watch_events WHERE applied=0 AND fence_reason='stale_round_connection'").fetchall()
        self.assertEqual(len(fenced), 1)
        self.assertEqual(fenced[0]["round_id"], first)
        self.assertEqual(json.loads(fenced[0]["event_json"])["object"]["metadata"]["uid"], "uid-ops")

    def test_watch_410_triggers_evidence_based_relist_without_guessing_rv(self):
        first = self.ingestor.synchronize("initial")
        pump(self.ingestor)
        progress_before = self.catalog.connection.execute(
            "SELECT progress_rv FROM scope_status WHERE scope='cluster'").fetchone()["progress_rv"]
        self.sim.admin("expire", {"conn_id": self.ingestor.streams["cluster"].conn_id})
        pump(self.ingestor, times=10)
        # 410 后进度 RV 保持原值，代码路径上没有任何 RV 递增/拼接。
        progress_after = self.catalog.connection.execute(
            "SELECT progress_rv FROM scope_status WHERE scope='cluster'").fetchone()["progress_rv"]
        self.assertEqual(progress_before, progress_after)
        second = self.ingestor.synchronize(self.ingestor.relist_reason)
        self.assertEqual(self.catalog.round_state(second)["state"], "promoted")
        pages = self.catalog.round_evidence(second)["list_pages"]
        self.assertTrue(pages)
        # 新轮次 Watch 的起始 RV 必须来自新 List 快照，而非凭空构造。
        pump(self.ingestor, times=20)
        new_conn = self.catalog.connection.execute(
            "SELECT start_rv FROM connections WHERE round_id=? LIMIT 1", (second,)).fetchone()
        self.assertIsNotNone(new_conn)
        snapshot_rvs = {p["snapshot_rv"] for p in pages if p["scope"] == "cluster"}
        self.assertIn(new_conn["start_rv"], snapshot_rvs)

    def test_resource_version_is_treated_as_opaque_even_when_tokens_are_not_orderable(self):
        # 采集器对 RV 只做相等判断与原样透传：字典序“更小”的新 RV 必须能覆盖旧值，
        # 同 RV 重放必须去重；任何数值/字典序比较都会让下面的断言失败。
        self.ingestor.synchronize("initial")
        pump(self.ingestor)
        conn_id = self.ingestor.streams["cluster"].conn_id
        # 当前对象 RV 是种子里的 rv-seed-2；注入字典序更小但更新的 RV。
        self.sim.admin("inject", {"conn_id": conn_id, "event": {"type": "MODIFIED", "object": {
            "metadata": {"uid": "uid-side", "name": "sidecar", "namespace": "lab",
                         "resourceVersion": "rv-0001-smallest"}}}})
        pump(self.ingestor)
        row = self.catalog.object_provenance("managed-a", "uid-side")["current"]
        self.assertEqual(row["object_rv"], "rv-0001-smallest")
        # 同 RV 重放被去重，而不是因比较逻辑被误判为更新或旧事件。
        self.sim.admin("inject", {"conn_id": conn_id, "event": {"type": "MODIFIED", "object": {
            "metadata": {"uid": "uid-side", "name": "sidecar", "namespace": "lab",
                         "resourceVersion": "rv-0001-smallest"}}}})
        pump(self.ingestor)
        self.assertGreaterEqual(self.catalog.fence_stats("managed-a").get("duplicate_rv", 0), 1)
        # 续接 Watch 时 RV 必须原样回传，不允许改写。
        requests = [r for r in self.store.requests if r["path"].endswith("/watch/pods")]
        self.assertTrue(requests)
        for request in requests:
            self.assertIn("resourceVersion", request["query"])
            self.assertNotIn("continue", request["query"])

    def test_bookmark_updates_progress_but_never_creates_a_business_object(self):
        self.ingestor.synchronize("initial")
        pump(self.ingestor, times=60)
        events = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM watch_events WHERE event_type='BOOKMARK' AND applied=1").fetchone()
        self.assertGreaterEqual(events["n"], 1)
        bookmark = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM watch_events WHERE event_type='BOOKMARK'").fetchone()
        # BOOKMARK 不写对象历史。
        history = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM object_history WHERE origin_kind='watch' "
            "AND object_rv IN (SELECT object_rv FROM watch_events WHERE event_type='BOOKMARK')").fetchone()
        self.assertEqual(history["n"], 0)
        self.assertEqual(bookmark["n"], events["n"])
        progress = self.catalog.connection.execute(
            "SELECT progress_rv FROM scope_status WHERE scope='cluster'").fetchone()
        self.assertTrue(progress["progress_rv"])

    def test_forbidden_namespace_is_marked_restricted_without_fabricating_deletes(self):
        round_id = self.ingestor.synchronize("initial")
        coverage = {c["scope"]: c for c in
                    self.catalog.coverage("managed-a", SCOPES + ["namespace:ghost"])}
        self.assertEqual(coverage["namespace:restricted"]["state"], "restricted")
        self.assertIn("403", coverage["namespace:restricted"]["restricted_reason"])
        self.assertEqual(coverage["namespace:restricted"]["evidence_round_id"], round_id)
        self.assertEqual(coverage["namespace:ghost"]["state"], "never_covered")
        # 受限范围没有任何 List 页证据，也没有对象被当作删除对账。
        pages = [p for p in self.catalog.round_evidence(round_id)["list_pages"]
                 if p["scope"] == "namespace:restricted"]
        self.assertEqual(pages, [])
        # 受限范围 Watch 也返回 403 时，状态保持 restricted 且不影响其他范围。
        pump(self.ingestor)
        again = {c["scope"]: c for c in self.catalog.coverage("managed-a", SCOPES)}
        self.assertEqual(again["namespace:restricted"]["state"], "restricted")
        self.assertEqual(again["cluster"]["state"], "watching")

    def test_view_and_provenance_survive_restart_via_persisted_round(self):
        round_id = self.ingestor.synchronize("initial")
        pump(self.ingestor)
        self.sim.admin("mutate", {"op": "put", "object": {
            "metadata": {"uid": "uid-side", "name": "sidecar", "namespace": "lab"}}})
        pump(self.ingestor)
        db_path = self.catalog.path
        self.ingestor.shutdown()
        self.catalog.close()
        reopened = Catalog(db_path)
        ingestor2 = Ingestor(KubeClient(self.sim.base_url), reopened, "managed-a", SCOPES, page_limit=2)
        resumed = ingestor2.resume()
        self.assertEqual(resumed, round_id)
        try:
            self.sim.admin("mutate", {"op": "put", "object": {
                "metadata": {"uid": "u-after-restart", "name": "reboot", "namespace": "lab"}}})
            pump(ingestor2)
            row = reopened.object_provenance("managed-a", "u-after-restart")["current"]
            self.assertEqual(row["origin_round_id"], round_id)
            self.assertEqual(row["origin_kind"], "watch")
            self.assertEqual({r["uid"] for r in reopened.objects("managed-a")},
                             {"uid-old", "uid-side", "uid-ops", "u-after-restart"})
        finally:
            ingestor2.shutdown()
            reopened.close()

    def test_abandoned_round_never_becomes_active_after_reopen(self):
        good = self.ingestor.synchronize("initial")
        self.sim.admin("flags", {"fail_continue": True})
        with self.assertRaises(RuntimeError):
            self.ingestor.synchronize("periodic")
        db_path = self.catalog.path
        self.ingestor.shutdown()
        self.catalog.close()
        reopened = Catalog(db_path)
        try:
            self.assertEqual(reopened.active_round("managed-a")["round_id"], good)
            state = reopened.resume_state("managed-a")
            self.assertEqual(state[0], good)
        finally:
            reopened.close()

    def test_provenance_queries_trace_every_object_to_list_or_watch_evidence(self):
        round_id = self.ingestor.synchronize("initial")
        pump(self.ingestor)
        self.sim.admin("mutate", {"op": "put", "object": {
            "metadata": {"uid": "uid-new", "name": "fresh", "namespace": "lab"}}})
        pump(self.ingestor)
        listed = self.catalog.object_provenance("managed-a", "uid-old")
        self.assertEqual(listed["history"][0]["origin_kind"], "list")
        self.assertEqual(listed["history"][0]["origin_round_id"], round_id)
        self.assertTrue(listed["history"][0]["origin_page_id"])
        watched = self.catalog.object_provenance("managed-a", "uid-new")
        self.assertEqual(watched["history"][0]["origin_kind"], "watch")
        self.assertTrue(watched["history"][0]["origin_event_id"])
        evidence = self.catalog.round_evidence(round_id)
        self.assertEqual({r["state"] for r in evidence["scope_results"]},
                         {"listed", "restricted"})
        # 原始 HTTP 归档与目录证据通过 capture_id 关联。
        for page in evidence["list_pages"]:
            if page["capture_id"] is not None:
                self.archive.body(page["capture_id"])

    def test_collector_is_read_only_and_preserves_raw_http_archive(self):
        self.ingestor.synchronize("initial")
        pump(self.ingestor)
        methods = {(r["method"], r["path"]) for r in self.sim.store.requests}
        self.assertTrue(methods)
        for method, path in methods:
            self.assertEqual(method, "GET")
            self.assertFalse(path.startswith("/__admin__"))
        captures = self.archive.captures("kubesim:managed-a")
        self.assertTrue(captures)
        self.assertTrue(all(c["status"] == 200 for c in captures))

    def test_scope_specific_delete_does_not_touch_other_scopes(self):
        self.ingestor.synchronize("initial")
        pump(self.ingestor)
        self.sim.admin("mutate", {"op": "delete", "namespace": "ops", "name": "batch"})
        pump(self.ingestor)
        self.assertEqual(self._row("uid-ops")["alive"], 0)
        self.assertEqual(self._row("uid-old")["alive"], 1)
        # 下一轮重列举对账只在覆盖范围内生效，ops 的删除被确认，lab 不受影响。
        next_round = self.ingestor.synchronize("periodic")
        self.assertEqual(self.catalog.round_state(next_round)["state"], "promoted")
        self.assertEqual(self._row("uid-ops")["alive"], 0)
        self.assertEqual(self._row("uid-old")["alive"], 1)


if __name__ == "__main__":
    unittest.main()
