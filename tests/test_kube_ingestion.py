import json
import tempfile
import unittest
from pathlib import Path

from supplier_rpa.archive import Archive
from supplier_rpa.catalog import Catalog
from supplier_rpa.ingestor import CLUSTER_SCOPE, Ingestor
from supplier_rpa.kubeclient import KubeClient
from supplier_rpa.kubesim import KubeSim, load_kube_scenario


class KubeIngestionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.archive_path = base / "raw.sqlite3"
        self.catalog_path = base / "catalog.sqlite3"
        self.sim = KubeSim(load_kube_scenario())
        self.archive = Archive(self.archive_path)
        self.catalog = Catalog(self.catalog_path)
        self.client = KubeClient(self.sim.base_url, timeout=3, page_limit=2)
        self.ingestor = Ingestor(self.client, self.archive, self.catalog, "managed-a")

    def tearDown(self):
        try:
            self.ingestor.stop()
        finally:
            self.catalog.close()
            self.archive.close()
            self.sim.close()
            self.temporary.cleanup()

    def _present_uids(self):
        return {row["uid"] for row in self.catalog.current_objects("managed-a")}

    def test_first_list_paginates_and_promotes_full_catalog(self):
        result = self.ingestor.run_round(pre_watch=False)
        self.assertTrue(result.promoted)
        # 分页 limit=2，5 个对象至少 3 页
        pages = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM list_pages WHERE round_id=?", (result.round_id,)
        ).fetchone()["n"]
        self.assertGreaterEqual(pages, 3)
        self.assertEqual(
            self._present_uids(),
            {"uid-old", "uid-clock", "uid-probe", "uid-locked", "uid-agent"},
        )
        # 同一快照所有页共享不透明 RV
        self.assertIsNotNone(self.catalog.round_snapshot_rv(result.round_id))
        coverage = self.catalog.coverage("managed-a")
        self.assertEqual([row["state"] for row in coverage], ["covered"])
        self.assertEqual(coverage[0]["scope"], CLUSTER_SCOPE)
        # 原始报文全部留档
        self.assertGreaterEqual(len(self.archive.captures()), pages)

    def test_same_name_recreate_keeps_new_lifetime_when_old_delete_returns_late(self):
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()

        # 同名对象删除后以新 UID 重建
        self.sim.delete("lab", "worker")
        self.sim.put("lab", "worker", uid="uid-new", spec={"role": "batch", "generation": 2})
        self.ingestor.drain()

        view = self.catalog.object_by_name("managed-a", "lab", "worker")
        self.assertEqual({row["uid"] for row in view}, {"uid-old", "uid-new"})
        states = {row["uid"]: row["state"] for row in view}
        self.assertEqual(states, {"uid-old": "deleted", "uid-new": "present"})

        # 旧 watch 连接（已栅栏化）回流针对旧 UID 的迟到删除：
        # 绝不能抹掉新 UID 的对象
        stale_connection = self.catalog.active_round("managed-a")
        self.catalog.fence_round_connections(stale_connection, "test fence")
        late_event = {
            "type": "DELETED",
            "object": {"metadata": {
                "uid": "uid-old", "name": "worker", "namespace": "lab",
                "resourceVersion": "rv-late-from-past",
            }},
        }
        capture_id = self.archive.append(
            "managed-a", "/late", 200,
            {"content-type": "application/json; stream=watch"},
            json.dumps(late_event).encode(),
        )
        staged_id = self.catalog.stage_event(
            stale_connection, "managed-a",
            self.catalog.connection.execute(
                "SELECT connection_id FROM watch_connections WHERE round_id=?",
                (stale_connection,),
            ).fetchone()["connection_id"],
            late_event, capture_id,
        )
        self.ingestor._ingest_event(
            type("H", (), {
                "connection_id": self.catalog.connection.execute(
                    "SELECT connection_id FROM watch_connections WHERE round_id=?",
                    (stale_connection,),
                ).fetchone()["connection_id"],
                "round_id": stale_connection,
            })(),
            late_event, capture_id,
        )
        self.assertTrue(self.catalog.stale_events("managed-a"))
        new_obj = next(
            row for row in self.catalog.object_by_name("managed-a", "lab", "worker")
            if row["uid"] == "uid-new"
        )
        self.assertEqual(new_obj["state"], "present")
        # 新 UID 上没有任何删除证据
        self.assertNotIn(
            "DELETED",
            [row["event_type"] for row in self.catalog.provenance("managed-a", "uid-new")],
        )

    def test_watch_410_triggers_grounded_relist_without_guessing_version(self):
        result = self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        token_before = self.catalog.resume_token("managed-a")
        self.assertIn(token_before, self.sim.issued_tokens)

        # 压缩历史，再让服务端主动断开当前 watch，下一次续接必为 410
        expired = self.sim.compact()
        self.assertIn(token_before, expired)
        for watcher_id in self.sim.list_watcher_ids():
            self.sim.close_watcher(watcher_id)
        self.ingestor.drain()

        rounds = self.catalog.rounds("managed-a")
        self.assertGreaterEqual(len(rounds), 2)
        self.assertTrue(all(row["status"] in ("promoted",) for row in rounds
                            if row["status"] == "promoted"))
        token_after = self.catalog.resume_token("managed-a")
        # 新 token 必须来自服务端签发，且引擎从未自行签发任何版本号
        self.assertIn(token_after, self.sim.issued_tokens)
        self.assertNotEqual(token_after, token_before)
        active = self.catalog.active_round("managed-a")
        self.assertGreater(active, result.round_id)
        # 归档里能找到 410 Status 原文
        status_captures = [
            cap for cap in self.archive.captures()
            if cap["status"] == 410
        ]
        self.assertTrue(status_captures)

    def test_resource_version_is_opaque_never_numeric_or_ordered(self):
        # 换一套刻意违反数值/字典序直觉的版本号
        sim = KubeSim(load_kube_scenario(), token_prefix="x-")
        client = KubeClient(sim.base_url, timeout=3, page_limit=10)
        ingestor = Ingestor(client, self.archive, self.catalog, "opaque-cluster")
        try:
            ingestor.run_round(pre_watch=False)
            ingestor.start()
            sim.put("lab", "clock", uid="uid-clock", spec={"role": "cron", "tick": 1})
            ingestor.drain()
            stored = {
                row["resource_version"]
                for row in self.catalog.current_objects("opaque-cluster")
            }
            for rv in stored:
                self.assertTrue(rv.startswith("x-"))
                int(rv.removeprefix("x-"))  # 序号本身是数字，但调用方只能存传
            # 引擎代码路径中不存在对 RV 的大小比较：发送参数原样回传
            watch_requests = [req for req in sim.requests if req["kind"] == "watch"]
            for request in watch_requests:
                rv = request["params"].get("resourceVersion", "")
                if rv:
                    self.assertIn(rv, sim.issued_tokens)
        finally:
            ingestor.stop()
            sim.close()

    def test_incomplete_pagination_is_not_announced_as_full_catalog(self):
        first = self.ingestor.run_round(pre_watch=False)
        before = self._present_uids()

        # 让下一轮 List 的第二页失败
        sim = self.sim
        # 先拿到第一页会返回的 continue token
        page = self.client.list_page(self.archive, "managed-a")
        token = page.continue_token
        sim.fail_next_continue(token)

        second = self.ingestor.run_round(pre_watch=False)
        self.assertFalse(second.promoted)
        failed = self.catalog.connection.execute(
            "SELECT status FROM rounds WHERE round_id=?", (second.round_id,)
        ).fetchone()
        self.assertEqual(failed["status"], "failed")
        # 当前视图仍是上一轮晋升的目录
        self.assertEqual(self.catalog.active_round("managed-a"), first.round_id)
        self.assertEqual(self._present_uids(), before)

    def test_events_arriving_during_pagination_are_replayed_without_loss(self):
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        token = self.catalog.resume_token("managed-a")
        self.assertIn(token, self.sim.issued_tokens)

        # 下一轮重列举期间，watch 先于 List 挂起；分页中途发生变更
        def mutate_during_page(namespace, params):
            if self.sim.requests[-1]["params"].get("continue"):
                if not getattr(self, "_mutated", False):
                    self._mutated = True
                    self.sim.put("lab", "probe", uid="uid-probe",
                                 spec={"role": "probe", "tick": 9})
        self.sim.page_hook = mutate_during_page
        try:
            result = self.ingestor.run_round(pre_watch=True)
        finally:
            self.sim.page_hook = None
        self.assertTrue(result.promoted)
        probe = next(
            row for row in self.catalog.current_objects("managed-a")
            if row["uid"] == "uid-probe"
        )
        # watch 事件的 rv 必须在溯源链中，且对象为最新
        prov = self.catalog.provenance("managed-a", "uid-probe")
        origins = [row["origin"] for row in prov]
        self.assertIn("watch", origins)
        self.assertIn("list", origins)
        self.assertEqual(probe["state"], "present")

    def test_forbidden_namespace_is_marked_restricted_without_fabricating_deletes(self):
        # 先做集群级全量，拿到 restricted 里的对象
        self.ingestor.run_round(pre_watch=False)
        self.assertIn("uid-locked", self._present_uids())

        # 采集范围改为按 namespace 列举，并对 restricted 403
        scoped = Ingestor(
            self.client, self.archive, self.catalog, "managed-a",
            scopes=["lab", "restricted", "ops"],
        )
        self.sim.forbid_namespace("restricted")
        result = scoped.run_round(pre_watch=False)
        self.assertTrue(result.promoted)

        coverage = {row["scope"]: row["state"] for row in scoped.catalog.coverage("managed-a")}
        self.assertEqual(coverage["restricted"], "restricted")
        self.assertEqual(coverage["lab"], "covered")
        # 受限 scope 的旧对象保持 present，没有被对账成删除
        locked = next(
            row for row in scoped.catalog.current_objects("managed-a", include_deleted=True)
            if row["uid"] == "uid-locked"
        )
        self.assertEqual(locked["state"], "present")
        self.assertNotIn(
            "RECONCILED_ABSENT",
            [row["event_type"] for row in scoped.catalog.provenance("managed-a", "uid-locked")],
        )
        # 403 原始 Status 也在归档里
        self.assertTrue(any(cap["status"] == 403 for cap in self.archive.captures()))

    def test_restart_resumes_from_persisted_opaque_token(self):
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        self.sim.put("ops", "agent", uid="uid-agent", spec={"role": "agent", "boot": 1})
        self.ingestor.drain()
        token = self.catalog.resume_token("managed-a")
        self.ingestor.stop()
        self.catalog.close()

        # 新进程：重新打开同一个库，不重新 List，直接续 watch
        reopened_catalog = Catalog(self.catalog_path)
        reopened = Ingestor(self.client, self.archive, reopened_catalog, "managed-a")
        try:
            self.assertEqual(reopened_catalog.active_round("managed-a"), 1)
            reopened.start()
            self.sim.put("lab", "clock", uid="uid-clock", spec={"role": "cron", "boot": 2})
            reopened.drain()
            clock = next(
                row for row in reopened_catalog.current_objects("managed-a")
                if row["uid"] == "uid-clock"
            )
            self.assertEqual(json.loads(clock["payload"])["spec"]["boot"], 2)
            watch_requests = [req for req in self.sim.requests if req["kind"] == "watch"]
            self.assertIn(
                token,
                [req["params"].get("resourceVersion") for req in watch_requests],
            )
        finally:
            reopened.stop()
            reopened_catalog.close()

    def test_duplicate_event_at_same_uid_and_rv_is_applied_once(self):
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        obj = self.sim.put("lab", "probe", uid="uid-probe", spec={"role": "probe"})
        rv = obj["metadata"]["resourceVersion"]
        self.ingestor.drain()

        before = len(self.catalog.provenance("managed-a", "uid-probe"))
        # 同 UID 同 RV 的重复 MODIFIED（模拟重连后重叠窗口）
        duplicate = {
            "type": "MODIFIED",
            "object": {"metadata": {
                "uid": "uid-probe", "name": "probe", "namespace": "lab",
                "resourceVersion": rv,
            }},
        }
        capture_id = self.archive.append(
            "managed-a", "/dup", 200,
            {"content-type": "application/json; stream=watch"},
            json.dumps(duplicate).encode(),
        )
        active = self.catalog.active_round("managed-a")
        handle = type("H", (), {
            "connection_id": self.catalog.connection.execute(
                "SELECT connection_id FROM watch_connections WHERE round_id=? AND active=1",
                (active,),
            ).fetchone()["connection_id"],
            "round_id": active,
        })()
        self.ingestor._ingest_event(handle, duplicate, capture_id)
        after = len(self.catalog.provenance("managed-a", "uid-probe"))
        self.assertEqual(before, after)

    def test_bookmark_is_progress_not_a_business_object(self):
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        marked = self.sim.bookmark()
        self.ingestor.drain()
        self.assertEqual(self.catalog.resume_token("managed-a"), marked)
        # 没有任何业务对象被写成 BOOKMARK
        rows = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM objects WHERE last_origin='watch' "
            "AND resource_version IN (SELECT resource_version FROM object_provenance "
            "WHERE event_type='BOOKMARK')"
        ).fetchone()
        self.assertEqual(rows["n"], 0)
        bookmark_prov = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM object_provenance WHERE event_type='BOOKMARK'"
        ).fetchone()
        self.assertGreaterEqual(bookmark_prov["n"], 1)

    def test_watch_expiry_during_pagination_fails_round_without_partial_catalog(self):
        # 重列举分页期间 watch 收到 410：事件交接不完整，整轮失败，
        # 不得用部分分页冒充全量。
        first = self.ingestor.run_round(pre_watch=False)
        token = self.catalog.resume_token("managed-a")

        def expire_on_second_page(namespace, params):
            if self.sim.requests[-1]["params"].get("continue"):
                if not getattr(self, "_expired_once", False):
                    self._expired_once = True
                    # 分页进行到第二页时终止挂起的 watch，事件交接不再完整。
                    for watcher_id in self.sim.list_watcher_ids():
                        self.sim.close_watcher(watcher_id)
        self.sim.page_hook = expire_on_second_page
        try:
            with self.assertRaises(Exception):
                self.ingestor.run_round(pre_watch=True)
        finally:
            self.sim.page_hook = None
        self.assertEqual(self.catalog.active_round("managed-a"), first.round_id)
        later = self.catalog.rounds("managed-a")[-1]
        self.assertEqual(later["status"], "failed")

    def test_no_code_path_constructs_a_resource_version(self):
        # 引擎写入目录的每个 RV 都必须能在模拟器已签发集合中找到。
        self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        self.sim.put("lab", "clock", uid="uid-clock", spec={"tick": 3})
        self.sim.delete("lab", "probe")
        self.sim.bookmark()
        self.ingestor.drain()
        rows = self.catalog.connection.execute(
            "SELECT DISTINCT resource_version FROM objects "
            "UNION SELECT DISTINCT resource_version FROM object_provenance "
            "UNION SELECT value FROM kv WHERE key LIKE 'resume_token:%'"
        ).fetchall()
        for row in rows:
            token = row[0]
            if token:
                self.assertIn(token, self.sim.issued_tokens)

    def test_provenance_and_coverage_answer_archive_questions(self):
        result = self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        self.sim.delete("lab", "probe")
        self.ingestor.drain()

        prov = self.catalog.provenance("managed-a", "uid-probe")
        self.assertTrue(any(row["origin"] == "list" and row["page_id"] for row in prov))
        self.assertTrue(any(row["origin"] == "watch" and row["event_type"] == "DELETED"
                            and row["capture_id"] for row in prov))
        # 每条证据都能回到原始 capture 报文
        for row in prov:
            if row["capture_id"]:
                self.archive.body(row["capture_id"])

        coverage = self.catalog.coverage("managed-a")
        self.assertEqual(
            [(row["scope"], row["state"]) for row in coverage],
            [(CLUSTER_SCOPE, "covered")],
        )
        # 未覆盖证据查询：没有任何 scope 记录时可识别为空
        self.assertEqual(Catalog(self.catalog_path).coverage("never-seen-cluster"), [])

    def test_rounds_are_fenced_so_old_objects_never_mix_into_new_view(self):
        first = self.ingestor.run_round(pre_watch=False)
        self.ingestor.start()
        # 新一轮重列举
        second = self.ingestor.run_round(pre_watch=True)
        self.assertGreater(second.round_id, first.round_id)
        # 上一轮连接全部失活
        stale = self.catalog.connection.execute(
            "SELECT count(*) AS n FROM watch_connections WHERE round_id=? AND active=1",
            (first.round_id,),
        ).fetchone()["n"]
        self.assertEqual(stale, 0)
        # 当前对象全部来自最近一轮或之后的 watch，不存在更早轮次残留
        for row in self.catalog.current_objects("managed-a"):
            self.assertGreaterEqual(row["last_event_round"], first.round_id)


if __name__ == "__main__":
    unittest.main()
