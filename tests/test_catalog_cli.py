"""目录采集命令行装配测试（不启动外部进程，复用本地模拟器）。"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from supplier_rpa import __main__
from supplier_rpa.catalog import Catalog
from supplier_rpa.kube_client import KubeClient
from supplier_rpa.catalog import Ingestor
from supplier_rpa.kube_sim import demo_store, serve


class CatalogCliTests(unittest.TestCase):
    def test_kube_demo_reports_every_design_boundary(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            __main__.catalog_demo()
        report = json.loads(buffer.getvalue())
        self.assertEqual(report["operation"], "catalog_demo")
        steps = {item["step"]: item for item in report["steps"]}
        self.assertEqual(steps["same_name_recreated"]["worker_lifecycles"],
                         [{"uid": "uid-new", "alive": True},
                          {"uid": "uid-old", "alive": False}])
        self.assertEqual(steps["watch_410_relist"]["new_round_state"], "promoted")
        self.assertEqual(steps["stale_connection_fenced"]["sidecar_alive"], 1)
        self.assertEqual(steps["stale_connection_fenced"]["fence_stats"]
                         .get("stale_round_connection"), 1)
        coverage = {item["scope"]: item["state"] for item in report["coverage"]}
        self.assertEqual(coverage["namespace:restricted"], "restricted")
        self.assertEqual(coverage["namespace:never-seen"], "never_covered")
        self.assertGreater(report["raw_captures"], 0)

    def test_sync_and_query_commands_round_trip_through_main(self):
        with serve(demo_store()) as simulator, tempfile.TemporaryDirectory() as temporary:
            catalog_path = Path(temporary) / "catalog.sqlite3"
            archive_path = Path(temporary) / "raw.sqlite3"
            sync_args = __main__.argparse.Namespace(
                base_url=simulator.base_url, cluster="managed-a", catalog=catalog_path,
                archive=archive_path, source="kubesim:managed-a",
                scope=["cluster", "namespace:restricted"], page_limit=2,
                read_timeout=0.05, watch_seconds=0.2)
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                __main__.run_catalog(sync_args)
            summary = json.loads(buffer.getvalue())
            self.assertEqual(summary["operation"], "catalog_sync")
            self.assertTrue(summary["objects"])

            round_id = summary["round_id"]
            query_args = __main__.argparse.Namespace(
                what="round", catalog=catalog_path, cluster="managed-a",
                uid=None, round_id=round_id, scope=None)
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                __main__.query_catalog(query_args)
            evidence = json.loads(buffer.getvalue())
            self.assertEqual(evidence["round"]["state"], "promoted")
            self.assertTrue(evidence["list_pages"])

    def test_ingestor_can_be_built_and_closed_without_a_round(self):
        with serve(demo_store()) as simulator, tempfile.TemporaryDirectory() as temporary:
            catalog = Catalog(Path(temporary) / "catalog.sqlite3")
            ingestor = Ingestor(KubeClient(simulator.base_url, read_timeout=0.05),
                                catalog, "managed-a", ["cluster"], page_limit=2)
            ingestor.shutdown()
            catalog.close()


if __name__ == "__main__":
    unittest.main()
