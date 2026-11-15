import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .origin import serve
from .kubeclient import KubeClient
from .kubesim import KubeSim, load_kube_scenario
from .catalog import Catalog
from .ingestor import Ingestor


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def kube_demo(archive_path, catalog_path):
    """本地 API 模拟器上的端到端只读目录演练。"""
    sim = KubeSim(load_kube_scenario())
    archive = Archive(archive_path)
    catalog = Catalog(catalog_path)
    report = {"cluster": "managed-a", "steps": []}
    try:
        ingestor = Ingestor(KubeClient(sim.base_url), archive, catalog, "managed-a")
        first = ingestor.run_round(pre_watch=False)
        report["steps"].append({"initial_list": first.as_dict()})

        ingestor.start()
        # 同名对象删除后以新 UID 重建
        sim.delete("lab", "worker")
        sim.put("lab", "worker", uid="uid-new", spec={"role": "batch", "generation": 2})
        sim.bookmark()
        ingestor.drain()

        # 压缩历史并断开旧 watch，下一次续接得到 410，触发有依据的重列举
        sim.compact()
        for watcher_id in sim.list_watcher_ids():
            sim.close_watcher(watcher_id)
        ingestor.drain()

        # 采集范围受限：某 namespace 返回 403
        sim.forbid_namespace("restricted")
        scoped = Ingestor(
            KubeClient(sim.base_url), archive, catalog, "managed-a",
            scopes=["lab", "restricted", "ops"],
        )
        restricted_round = scoped.run_round(pre_watch=False)
        report["steps"].append({"scoped_relist": restricted_round.as_dict()})

        report["current_view"] = [
            {key: row[key] for key in ("namespace", "name", "uid", "state", "last_origin")}
            for row in catalog.current_objects("managed-a", include_deleted=True)
        ]
        report["name_lookup_worker"] = [
            {"uid": row["uid"], "state": row["state"]}
            for row in catalog.object_by_name("managed-a", "lab", "worker")
        ]
        report["coverage"] = catalog.coverage("managed-a")
        report["stale_events_quarantined"] = len(catalog.stale_events("managed-a"))
        report["provenance_worker_new"] = catalog.provenance("managed-a", "uid-new")
        report["rounds"] = catalog.rounds("managed-a")
        ingestor.stop()
        scoped.stop()
    finally:
        catalog.close()
        archive.close()
        sim.close()
    return report


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与归档")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    kube = subcommands.add_parser("kube-demo")
    kube.add_argument("--archive", type=Path)
    kube.add_argument("--catalog", type=Path)
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "kube-demo":
        if args.archive and args.catalog:
            report = kube_demo(args.archive, args.catalog)
        else:
            with tempfile.TemporaryDirectory() as temporary:
                report = kube_demo(
                    Path(temporary) / "raw.sqlite3",
                    Path(temporary) / "catalog.sqlite3",
                )
        print(json.dumps({"operation": "kube_catalog", **report}, ensure_ascii=False, indent=2, default=str))
    else:
        from .origin import load_scenario
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
