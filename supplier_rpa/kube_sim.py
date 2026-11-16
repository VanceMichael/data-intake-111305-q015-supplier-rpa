"""本地 Kubernetes API 模拟器。

只提供只读采集所需的 List / Watch / Status 子集，外加 ``/__admin__`` 管控
端点，用于在单进程内复现分页、断线、410、403 以及旧连接迟到事件等交错情况。
模拟器不接受任何业务写操作；管控端点只作用于本进程内存状态，不模拟部署。

resourceVersion 在对外协议里始终是不透明字符串：默认令牌随机生成、不保证
字典序或数值顺序，采集方只能原样保存与回传。模拟器内部另用单调下标维护
事件历史，仅用于决定 Watch 回放区间和 410 判定，该下标从不对外暴露。
"""

import json
import secrets
import threading
import uuid
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from urllib.parse import parse_qs, urlsplit

DEFAULT_HISTORY_LIMIT = 50
BOOKMARK_INTERVAL = 0.03


def _new_token():
    # 随机令牌天然不可排序，强制采集方按不透明标识使用 resourceVersion。
    return "rv-" + secrets.token_hex(4)


class Watcher:
    """一条 Watch 连接及其待投递队列。"""

    def __init__(self, conn_id, scope):
        self.conn_id = conn_id
        self.scope = scope
        self.queue = Queue()
        self.alive = True
        self.started = threading.Event()


class KubeStore:
    """内存资源存储与事件历史，线程安全。"""

    def __init__(self, history_limit=DEFAULT_HISTORY_LIMIT, rv_factory=None):
        self.lock = threading.RLock()
        self.objects = {}
        self.history = deque()
        self.counter = 0
        self.tokens = {}
        self.head_rv = None
        self.forbidden = set()
        self.page_limit = 2
        self.fail_continue = False
        self.rv_mismatch = False
        self.history_limit = history_limit
        self.rv_factory = rv_factory or _new_token
        self.watchers = {}
        self.requests = []
        self.page_callbacks = []
        self.list_snapshots = {}
        self.snapshot_rv_of = {}
        # 初始 head RV 不对应任何历史事件（下标 0），空存储 List 用它做交接令牌。
        self.head_rv = self.rv_factory()
        self.tokens[self.head_rv] = 0

    # -- 内部版本管理 -------------------------------------------------------

    def _advance_head(self, rv=None):
        rv = rv or self.rv_factory()
        self.head_rv = rv
        return rv

    def _append_history(self, event_type, obj, scope):
        self.counter += 1
        rv = obj["metadata"]["resourceVersion"]
        entry = {"index": self.counter, "type": event_type, "object": obj, "rv": rv, "scope": scope}
        self.history.append(entry)
        # 每个 RV 关联其自身事件的位置；Watch 从该 RV 开始时回放其后的事件。
        self.tokens[rv] = self.counter
        self.head_rv = rv
        while len(self.history) > self.history_limit:
            expired = self.history.popleft()
            if self.tokens.get(expired["rv"]) == expired["index"]:
                del self.tokens[expired["rv"]]
        self._fanout(entry)

    def _scope_of(self, namespace):
        return "cluster" if not namespace else "namespace:" + namespace

    def _fanout(self, entry):
        for watcher in self.watchers.values():
            if not watcher.alive:
                continue
            if entry["scope"].startswith("namespace:") and entry["scope"].split(":", 1)[1] in self.forbidden:
                continue
            if watcher.scope == "cluster" or watcher.scope == entry["scope"]:
                watcher.queue.put(("event", {"type": entry["type"], "object": entry["object"]}))

    # -- 测试种子与管控操作 -------------------------------------------------

    def seed(self, pods):
        with self.lock:
            for pod in pods:
                self.put(pod)

    def put(self, pod):
        with self.lock:
            key = (pod["metadata"]["namespace"], pod["metadata"]["name"])
            existing = self.objects.get(key)
            if existing is None:
                event_type, pod = "ADDED", json.loads(json.dumps(pod))
                pod["metadata"].setdefault("uid", str(uuid.uuid4()))
            else:
                event_type = "MODIFIED"
                pod = json.loads(json.dumps(pod))
                pod["metadata"]["uid"] = existing["metadata"]["uid"]
            rv = pod["metadata"].get("resourceVersion") or self.rv_factory()
            pod["metadata"]["resourceVersion"] = rv
            self.objects[key] = pod
            self._advance_head(rv)
            self._append_history(event_type, pod, self._scope_of(key[0]))
            return pod

    def delete(self, namespace, name):
        with self.lock:
            key = (namespace, name)
            existing = self.objects.pop(key, None)
            if existing is None:
                return None
            tombstone = json.loads(json.dumps(existing))
            tombstone["metadata"]["resourceVersion"] = self._advance_head()
            self._append_history("DELETED", tombstone, self._scope_of(namespace))
            return tombstone

    def set_forbidden(self, namespace, forbidden):
        with self.lock:
            if forbidden:
                self.forbidden.add(namespace)
            else:
                self.forbidden.discard(namespace)

    # -- List ---------------------------------------------------------------

    def list(self, namespace, limit, continue_token):
        """返回 (status, payload)。分页共享同一个快照 resourceVersion。"""
        with self.lock:
            if namespace and namespace in self.forbidden:
                return 403, {"kind": "Status", "status": "Failure", "code": 403, "reason": "Forbidden",
                             "message": "namespace %s is outside the read-only grant" % namespace}
            if continue_token:
                if self.fail_continue:
                    return 500, {"kind": "Status", "status": "Failure", "code": 500, "reason": "InternalError",
                                 "message": "synthetic paging failure"}
                try:
                    token = json.loads(continue_token)
                    offset, snapshot_id = int(token["off"]), str(token["snap"])
                except (ValueError, KeyError, TypeError):
                    return 400, {"kind": "Status", "status": "Failure", "code": 400, "reason": "BadRequest"}
                rows = self.list_snapshots[snapshot_id]
                snapshot_rv = self.snapshot_rv_of[snapshot_id]
            else:
                offset = 0
                rows = sorted(
                    (pod for pod in self.objects.values()
                     if (not namespace or pod["metadata"]["namespace"] == namespace)
                     and pod["metadata"]["namespace"] not in self.forbidden),
                    key=lambda pod: (pod["metadata"]["namespace"], pod["metadata"]["name"]),
                )
                rows = [json.loads(json.dumps(pod)) for pod in rows]
                # 每次首屏调用都是独立快照：生成快照专属 RV，映射到当前历史位置。
                # 分页期间 head 再前进也不改变交接点；Watch 从该 RV 回放其后的事件。
                snapshot_id = "snap-" + uuid.uuid4().hex[:12]
                snapshot_rv = self.rv_factory()
                self.tokens[snapshot_rv] = self.counter
                self.list_snapshots[snapshot_id] = rows
                self.snapshot_rv_of[snapshot_id] = snapshot_rv
            page = rows[offset:offset + limit]
            next_offset = offset + len(page)
            next_token = ""
            if next_offset < len(rows):
                next_token = json.dumps({"off": next_offset, "snap": snapshot_id}, separators=(",", ":"))
            page_rv = self.rv_factory() if self.rv_mismatch and offset else snapshot_rv
            payload = {
                "kind": "PodList",
                "metadata": {"resourceVersion": page_rv, "continue": next_token},
                "items": [json.loads(json.dumps(pod)) for pod in page],
            }
            return 200, payload

    # -- Watch --------------------------------------------------------------

    def open_watch(self, scope, resource_version, allow_bookmarks):
        """返回 (status, watcher_or_None)。未知 rv 直接 410。"""
        with self.lock:
            if scope.startswith("namespace:") and scope.split(":", 1)[1] in self.forbidden:
                return 403, None
            if resource_version and resource_version not in self.tokens:
                return 410, None
            conn_id = "conn-" + uuid.uuid4().hex[:12]
            watcher = Watcher(conn_id, scope)
            self.watchers[conn_id] = watcher
            # K8s 语义：从 rv 开始 Watch，回放该 rv 对应事件之后的事件。
            start_index = self.tokens.get(resource_version, 0)
            for entry in self.history:
                if entry["index"] <= start_index:
                    continue
                if entry["scope"].startswith("namespace:") and entry["scope"].split(":", 1)[1] in self.forbidden:
                    continue
                if scope == "cluster" or scope == entry["scope"]:
                    watcher.queue.put(("event", {"type": entry["type"], "object": entry["object"]}))
            if allow_bookmarks:
                watcher.queue.put(("event", {"type": "BOOKMARK",
                                             "object": {"kind": "Pod", "metadata": {"resourceVersion": self.head_rv}}}))
            watcher.started.set()
            return 200, watcher

    def bookmark(self, watcher):
        with self.lock:
            return {"type": "BOOKMARK", "object": {"kind": "Pod", "metadata": {"resourceVersion": self.head_rv}}}

    # -- 管控端点：断线 / 过期 / 原始事件注入 --------------------------------

    def _watcher(self, conn_id):
        watcher = self.watchers.get(conn_id)
        if watcher is None:
            raise KeyError(conn_id)
        return watcher

    def inject(self, conn_id, event):
        with self.lock:
            self._watcher(conn_id).queue.put(("event", event))

    def expire(self, conn_id, code=410):
        reason = "Expired" if code == 410 else "Forbidden"
        status = {"kind": "Status", "status": "Failure", "code": code, "reason": reason,
                  "message": "synthetic watch status from simulator control plane"}
        with self.lock:
            self._watcher(conn_id).queue.put(("status", status))

    def drop(self, conn_id):
        with self.lock:
            self._watcher(conn_id).queue.put(("close", None))

    def record_request(self, method, path, query):
        with self.lock:
            self.requests.append({"method": method, "path": path, "query": query})

    def fire_page_callbacks(self, namespace, page_index):
        for callback in list(self.page_callbacks):
            callback(namespace, page_index)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string, *args):
        return None

    def _send(self, status, payload, extra_headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _scope_paths(self, path):
        # 返回 (namespace, is_watch)；cluster 级 namespace 为 ""。
        if path == "/api/v1/pods":
            return "", False
        if path == "/api/v1/watch/pods":
            return "", True
        prefix = "/api/v1/namespaces/"
        if path.startswith(prefix):
            rest = path[len(prefix):]
            if rest.endswith("/pods"):
                return rest[: -len("/pods")], False
            if rest.endswith("/pods/watch"):
                return rest[: -len("/pods/watch")], True
        return None, None

    def do_GET(self):
        split = urlsplit(self.path)
        path, query = split.path, parse_qs(split.query)
        self.server.store.record_request("GET", path, query)
        namespace, is_watch = self._scope_paths(path)
        if namespace is None:
            self._send(404, {"kind": "Status", "code": 404, "reason": "NotFound"})
            return
        scope = "cluster" if not namespace else "namespace:" + namespace
        if is_watch:
            self._serve_watch(scope, query)
        else:
            self._serve_list(namespace, query)

    def _serve_list(self, namespace, query):
        limit = int(query.get("limit", [str(self.server.store.page_limit)])[0])
        continue_token = query.get("continue", [""])[0]
        # 回调在每页快照查询前触发，用于复现“分页期间发生的变更”；
        # 已开始的分页快照不受影响，该变更应由 Watch 从交接 RV 补齐。
        page_index = 0 if not continue_token else json.loads(continue_token)["off"]
        self.server.store.fire_page_callbacks(namespace, page_index)
        status, payload = self.server.store.list(namespace, limit, continue_token)
        self._send(status, payload)

    def _serve_watch(self, scope, query):
        rv = query.get("resourceVersion", [""])[0]
        allow_bookmarks = query.get("allowWatchBookmarks", ["false"])[0].lower() == "true"
        status, watcher = self.server.store.open_watch(scope, rv, allow_bookmarks)
        if status != 200:
            self._send(status, watcher if isinstance(watcher, dict) else {
                "kind": "Status", "status": "Failure", "code": status,
                "reason": "Expired" if status == 410 else "Forbidden"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Kubesim-Connection-Id", watcher.conn_id)
        # 行分隔 JSON 长连接：不声明长度、连接关闭即流结束（断线由采集状态机处理）。
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            while True:
                try:
                    kind, payload = watcher.queue.get(timeout=BOOKMARK_INTERVAL)
                except Exception:
                    # 空闲心跳：仅推送 BOOKMARK 进度，不产生业务对象。
                    if not allow_bookmarks:
                        continue
                    kind, payload = "event", self.server.store.bookmark(watcher)
                if kind == "close":
                    return
                if kind == "status":
                    self.wfile.write((json.dumps({"type": "ERROR", "object": payload}, ensure_ascii=False) + "\n").encode())
                    self.wfile.flush()
                    return
                self.wfile.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            return
        finally:
            watcher.alive = False
            self.close_connection = True

    def do_POST(self):
        split = urlsplit(self.path)
        path = split.path
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        self.server.store.record_request("POST", path, parse_qs(split.query))
        if not path.startswith("/__admin__/"):
            self._send(404, {"kind": "Status", "code": 404, "reason": "NotFound"})
            return
        try:
            body = json.loads(raw or b"{}")
            store = self.server.store
            if path == "/__admin__/mutate":
                if body.get("op") == "delete":
                    result = store.delete(body["namespace"], body["name"])
                else:
                    result = store.put(body["object"])
                self._send(200, {"ok": True, "object": result})
            elif path == "/__admin__/inject":
                store.inject(body["conn_id"], body["event"])
                self._send(200, {"ok": True})
            elif path == "/__admin__/expire":
                store.expire(body["conn_id"], int(body.get("code", 410)))
                self._send(200, {"ok": True})
            elif path == "/__admin__/drop":
                store.drop(body["conn_id"])
                self._send(200, {"ok": True})
            elif path == "/__admin__/forbid":
                store.set_forbidden(body["namespace"], bool(body.get("forbidden", True)))
                self._send(200, {"ok": True})
            elif path == "/__admin__/flags":
                store.fail_continue = bool(body.get("fail_continue", store.fail_continue))
                store.rv_mismatch = bool(body.get("rv_mismatch", store.rv_mismatch))
                store.page_limit = int(body.get("page_limit", store.page_limit))
                self._send(200, {"ok": True})
            else:
                self._send(404, {"kind": "Status", "code": 404, "reason": "NotFound"})
        except KeyError as missing:
            self._send(404, {"kind": "Status", "code": 404, "reason": "NotFound", "message": str(missing)})
        except (ValueError, KeyError, TypeError) as bad:
            self._send(422, {"kind": "Status", "code": 422, "reason": "Invalid", "message": str(bad)})


class KubeSimulator(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, store=None):
        self.store = store or KubeStore()
        super().__init__(("127.0.0.1", 0), Handler)

    @property
    def base_url(self):
        return "http://127.0.0.1:" + str(self.server_port)

    def admin(self, path, body):
        import http.client

        conn = http.client.HTTPConnection("127.0.0.1", self.server_port, timeout=3)
        payload = json.dumps(body).encode()
        conn.request("POST", "/__admin__/" + path, payload, {"Content-Type": "application/json"})
        response = conn.getresponse()
        data = json.loads(response.read())
        conn.close()
        if response.status != 200:
            raise RuntimeError("admin call failed: %s" % data)
        return data


@contextmanager
def serve(store=None):
    server = KubeSimulator(store)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def demo_store():
    """演示与文档使用的确定性种子。"""
    store = KubeStore()
    store.seed([
        {"metadata": {"uid": "uid-old", "name": "worker", "namespace": "lab", "resourceVersion": "rv-seed-1"}},
        {"metadata": {"uid": "uid-side", "name": "sidecar", "namespace": "lab", "resourceVersion": "rv-seed-2"}},
        {"metadata": {"uid": "uid-ops", "name": "batch", "namespace": "ops", "resourceVersion": "rv-seed-3"}},
        {"metadata": {"uid": "uid-denied", "name": "locked", "namespace": "restricted", "resourceVersion": "rv-seed-4"}},
    ])
    store.set_forbidden("restricted", True)
    return store
