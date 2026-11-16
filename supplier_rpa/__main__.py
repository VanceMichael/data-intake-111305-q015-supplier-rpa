import argparse
import json
import tempfile
import time
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .origin import serve


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


def _parse_scopes(values):
    scopes = []
    for value in values or ["cluster"]:
        scopes.extend(part.strip() for part in value.split(",") if part.strip())
    return scopes or ["cluster"]


def run_catalog(args):
    """对本地或指定模拟器执行一次同步并保持 Watch，输出当前视图与覆盖证据。"""
    from .catalog import Catalog, Ingestor
    from .kube_client import KubeClient

    scopes = _parse_scopes(args.scope)
    archive = Archive(args.archive) if args.archive else None
    catalog = Catalog(args.catalog)
    ingestor = Ingestor(KubeClient(args.base_url, read_timeout=args.read_timeout),
                        catalog, args.cluster, scopes, archive=archive,
                        source=args.source, page_limit=args.page_limit)
    try:
        round_id = ingestor.synchronize("cli-sync")
        deadline = time.time() + args.watch_seconds
        while time.time() < deadline:
            ingestor.pump_once()
        summary = {
            "operation": "catalog_sync",
            "cluster": args.cluster,
            "round_id": round_id,
            "scopes": scopes,
            "objects": [
                {"uid": row["uid"], "name": row["name"], "namespace": row["namespace"],
                 "alive": bool(row["alive"]), "origin_kind": row["origin_kind"],
                 "origin_round_id": row["origin_round_id"], "object_rv": row["object_rv"]}
                for row in catalog.objects(args.cluster, alive_only=False)
            ],
            "coverage": catalog.coverage(args.cluster, scopes),
            "fence_stats": catalog.fence_stats(args.cluster),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        ingestor.shutdown()
        catalog.close()
        if archive is not None:
            archive.close()


def query_catalog(args):
    """归档人员查询：对象溯源、轮次证据或覆盖范围。"""
    from .catalog import Catalog

    catalog = Catalog(args.catalog)
    try:
        if args.what == "object":
            print(json.dumps(catalog.object_provenance(args.cluster, args.uid),
                             ensure_ascii=False, indent=2))
        elif args.what == "round":
            print(json.dumps(catalog.round_evidence(args.round_id), ensure_ascii=False, indent=2))
        else:
            print(json.dumps(catalog.coverage(args.cluster, _parse_scopes(args.scope)),
                             ensure_ascii=False, indent=2))
    finally:
        catalog.close()


def run_kube_serve(_args):
    from .kube_sim import demo_store, serve as kube_serve

    with kube_serve(demo_store()) as simulator:
        print(json.dumps({"base_url": simulator.base_url}, ensure_ascii=False), flush=True)
        try:
            import threading
            threading.Event().wait()
        except KeyboardInterrupt:
            pass


def catalog_demo():
    """本地端到端演示：分页 List、同名重建、410 重列举、旧连接围栏与 403 受限。"""
    from .catalog import Catalog, Ingestor
    from .kube_client import KubeClient
    from .kube_sim import demo_store, serve as kube_serve

    with tempfile.TemporaryDirectory() as temporary, kube_serve(demo_store()) as simulator:
        root = Path(temporary)
        catalog = Catalog(root / "catalog.sqlite3")
        archive = Archive(root / "raw.sqlite3")
        scopes = ["cluster", "namespace:ops", "namespace:restricted"]
        ingestor = Ingestor(KubeClient(simulator.base_url, read_timeout=0.05),
                            catalog, "managed-a", scopes, archive=archive,
                            source="kubesim:managed-a", page_limit=2)
        report = {"steps": []}
        try:
            first = ingestor.synchronize("initial")
            for _ in range(30):
                ingestor.pump_once()
            report["steps"].append({"step": "initial_list", "round_id": first,
                                    "alive_uids": sorted(r["uid"] for r in catalog.objects("managed-a"))})

            # 同名对象删除并以新 UID 重建。
            simulator.admin("mutate", {"op": "delete", "namespace": "lab", "name": "worker"})
            simulator.admin("mutate", {"op": "put", "object": {
                "metadata": {"uid": "uid-new", "name": "worker", "namespace": "lab"}}})
            for _ in range(40):
                ingestor.pump_once()
            report["steps"].append({
                "step": "same_name_recreated",
                "worker_lifecycles": [
                    {"uid": row["uid"], "alive": bool(row["alive"])}
                    for row in catalog.objects("managed-a", alive_only=False) if row["name"] == "worker"],
            })

            # Watch 410：不猜 RV，重新列举并原子切换目录。
            simulator.admin("expire", {"conn_id": ingestor.streams["cluster"].conn_id})
            for _ in range(10):
                ingestor.pump_once()
            second = ingestor.synchronize(ingestor.relist_reason)
            for _ in range(30):
                ingestor.pump_once()
            report["steps"].append({"step": "watch_410_relist", "previous_round": first,
                                    "new_round": second,
                                    "new_round_state": catalog.round_state(second)["state"]})

            # 旧连接迟到的删除事件只能入证，不能改动当前视图。
            stale = catalog.connection.execute(
                "SELECT conn_id FROM connections WHERE round_id=? AND state='superseded' LIMIT 1",
                (first,)).fetchone()
            if stale is not None:
                simulator.admin("inject", {"conn_id": stale["conn_id"], "event": {
                    "type": "DELETED", "object": {"metadata": {
                        "uid": "uid-side", "name": "sidecar", "namespace": "lab",
                        "resourceVersion": "rv-late-demo"}}}})
                for _ in range(30):
                    ingestor.pump_once()
            report["steps"].append({"step": "stale_connection_fenced",
                                    "sidecar_alive": catalog.object_provenance(
                                        "managed-a", "uid-side")["current"]["alive"],
                                    "fence_stats": catalog.fence_stats("managed-a")})
            report["coverage"] = catalog.coverage("managed-a", scopes + ["namespace:never-seen"])
            report["raw_captures"] = len(archive.captures("kubesim:managed-a"))
        finally:
            ingestor.shutdown()
            catalog.close()
            archive.close()
        print(json.dumps({"operation": "catalog_demo", **report}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与归档")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)

    kube_demo = subcommands.add_parser("kube-demo", help="本地 Kubernetes List/Watch 目录演示")
    kube_demo.set_defaults(func=lambda _args: catalog_demo())

    kube_server = subcommands.add_parser("kube-serve", help="启动本地只读 Kubernetes API 模拟器")
    kube_server.set_defaults(func=run_kube_serve)

    sync = subcommands.add_parser("kube-sync", help="对只读 Kubernetes 兼容端点执行 List/Watch 采集")
    sync.add_argument("--base-url", required=True)
    sync.add_argument("--cluster", required=True)
    sync.add_argument("--catalog", type=Path, required=True)
    sync.add_argument("--archive", type=Path)
    sync.add_argument("--source")
    sync.add_argument("--scope", action="append", help="cluster 或 namespace:名称，可重复或逗号分隔")
    sync.add_argument("--page-limit", type=int, default=2)
    sync.add_argument("--read-timeout", type=float, default=0.1)
    sync.add_argument("--watch-seconds", type=float, default=1.0)
    sync.set_defaults(func=run_catalog)

    query = subcommands.add_parser("kube-query", help="查询目录对象溯源、轮次证据与覆盖范围")
    query.add_argument("what", choices=["object", "round", "coverage"])
    query.add_argument("--catalog", type=Path, required=True)
    query.add_argument("--cluster", required=True)
    query.add_argument("--uid")
    query.add_argument("--round-id")
    query.add_argument("--scope", action="append")
    query.set_defaults(func=query_catalog)

    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command in ("kube-demo", "kube-sync", "kube-query", "kube-serve"):
        args.func(args)
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
