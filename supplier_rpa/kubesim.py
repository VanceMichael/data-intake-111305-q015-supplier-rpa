"""本地 Kubernetes API 子集模拟器。

只模拟只读 List / Watch / Status 语义，不执行任何部署或变更：

- List 支持 ``limit`` / ``continue`` 分页，同一快照的所有页共享同一个
  不透明 resourceVersion；
- Watch 以 NDJSON 逐条推送 ADDED / MODIFIED / DELETED / BOOKMARK；
- 可对任意 resourceVersion 注入压缩（后续 Watch 返回 410 Gone）；
- 可按 namespace 注入 403 Forbidden；
- 所有已签发的 resourceVersion 都登记在 ``issued_tokens``，便于测试
  验证客户端从不伪造版本号。

模拟器与真实集群无关，状态全部保存在进程内存中。
"""

import base64
import copy
import json
import queue
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

_POD_PATH = re.compile(r"^/api/v1/(?:namespaces/([^/]+)/)?pods$")


def _encode_continue(offset, snapshot_rv):
    raw = json.dumps({"off": offset, "rv": snapshot_rv}, ensure_ascii=False).encode()
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_continue(token):
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(raw)
        if not isinstance(payload.get("off"), int) or not isinstance(payload.get("rv"), str):
            raise ValueError
        return payload["off"], payload["rv"]
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


class KubeSim(ThreadingHTTPServer):
    """带可变内存存储的 Kubernetes API 模拟器。"""

    daemon_threads = True

    def __init__(self, scenario=None, token_prefix="rv-"):
        self.state_lock = threading.Lock()
        self.token_prefix = token_prefix
        self.token_seq = 0
        self.current_rv = None
        self.issued_tokens = set()
        self.expired_tokens = set()
        self.objects = {}
        self.forbidden_namespaces = set()
        self.fail_continue_tokens = set()
        self.watchers = {}
        self.watcher_seq = 0
        self.requests = []
        self.page_hook = None
        self._scenario = scenario or {}
        self._load_scenario(self._scenario)
        super().__init__(("127.0.0.1", 0), _KubeHandler)
        self._serve_thread = threading.Thread(
            target=self.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        )
        self._serve_thread.start()

    def _load_scenario(self, scenario):
        self.page_limit_default = int(scenario.get("page_limit", 500))
        self.forbidden_namespaces = set(scenario.get("forbidden_namespaces", []))
        for item in scenario.get("objects", []):
            self._put_locked(item["namespace"], item["name"], item.get("uid"), item.get("spec", {}), False)

    @property
    def base_url(self):
        return "http://127.0.0.1:" + str(self.server_port)

    def close(self):
        """停止服务循环并关闭监听套接字；处理线程为 daemon。"""
        self.shutdown()
        self.server_close()
        self._serve_thread.join(timeout=2)

    def _next_token_locked(self):
        self.token_seq += 1
        token = f"{self.token_prefix}{self.token_seq}"
        self.issued_tokens.add(token)
        # 压缩之后新签发的版本重新有效；历史 token 保持过期。
        self.expired_tokens.discard(token)
        return token

    def _put_locked(self, namespace, name, uid, spec, emit_event):
        key = (namespace, name)
        existing = self.objects.get(key)
        token = self._next_token_locked()
        if existing is None:
            uid = uid or f"uid-{namespace}-{name}-{self.token_seq}"
            event_type = "ADDED"
        else:
            uid = uid or existing["metadata"]["uid"]
            event_type = "MODIFIED"
        obj = {
            "kind": "Pod",
            "metadata": {
                "uid": uid,
                "name": name,
                "namespace": namespace,
                "resourceVersion": token,
            },
            "spec": copy.deepcopy(spec),
        }
        self.objects[key] = obj
        self.current_rv = token
        if emit_event:
            self._fanout_locked(event_type, obj)
        return obj

    def put(self, namespace, name, uid=None, spec=None):
        with self.state_lock:
            return self._put_locked(namespace, name, uid, spec or {}, True)

    def delete(self, namespace, name):
        with self.state_lock:
            key = (namespace, name)
            existing = self.objects.pop(key, None)
            if existing is None:
                raise KeyError((namespace, name))
            token = self._next_token_locked()
            tombstone = copy.deepcopy(existing)
            tombstone["metadata"]["resourceVersion"] = token
            self.current_rv = token
            self._fanout_locked("DELETED", tombstone)
            return tombstone

    def bookmark(self):
        with self.state_lock:
            if self.current_rv is None:
                self.current_rv = self._next_token_locked()
            event = {
                "type": "BOOKMARK",
                "object": {"kind": "Pod", "metadata": {"resourceVersion": self.current_rv}},
            }
            self._dispatch_locked(event)
            return self.current_rv

    def _dispatch_locked(self, event):
        dead = []
        for watcher_id, watcher in self.watchers.items():
            try:
                watcher["queue"].put_nowait(event)
            except queue.Full:
                dead.append(watcher_id)
        for watcher_id in dead:
            self.watchers.pop(watcher_id, None)

    def _fanout_locked(self, event_type, body):
        event = {"type": event_type, "object": copy.deepcopy(body)}
        self._dispatch_locked(event)

    def expire_token(self, token):
        with self.state_lock:
            if token not in self.issued_tokens:
                raise KeyError(token)
            self.expired_tokens.add(token)

    def compact(self):
        """模拟 etcd 压缩：所有已签发的历史 token 全部不可续接。

        服务端仍可在更新后的当前版本上接受全新 List；客户端持有的旧
        resume token 再发起 Watch 时得到 410。
        """
        with self.state_lock:
            self.expired_tokens = set(self.issued_tokens)
            return set(self.expired_tokens)

    def forbid_namespace(self, namespace):
        with self.state_lock:
            self.forbidden_namespaces.add(namespace)

    def fail_next_continue(self, token):
        with self.state_lock:
            self.fail_continue_tokens.add(token)

    def close_watcher(self, watcher_id):
        with self.state_lock:
            watcher = self.watchers.get(watcher_id)
            if watcher is not None:
                watcher["queue"].put(None)

    def list_watcher_ids(self):
        with self.state_lock:
            return list(self.watchers)

    def snapshot(self):
        with self.state_lock:
            return copy.deepcopy(list(self.objects.values())), self.current_rv

    def _list(self, params, namespace):
        limit_text = params.get("limit", [""])[0]
        limit = int(limit_text) if limit_text else self.page_limit_default
        continue_token = params.get("continue", [""])[0]
        with self.state_lock:
            if namespace is not None and namespace in self.forbidden_namespaces:
                return 403, {"kind": "Status", "code": 403, "reason": "Forbidden",
                             "message": f"namespace {namespace} is forbidden"}
            if continue_token:
                if continue_token in self.fail_continue_tokens:
                    self.fail_continue_tokens.discard(continue_token)
                    return 503, {"kind": "Status", "code": 503, "reason": "InternalError",
                                 "message": "injected pagination failure"}
                decoded = _decode_continue(continue_token)
                if decoded is None:
                    return 400, {"kind": "Status", "code": 400, "reason": "BadRequest"}
                offset, snapshot_rv = decoded
                if snapshot_rv in self.expired_tokens:
                    # 压缩后旧 continue 令牌的快照不再可续：推进到新版本。
                    snapshot_rv = self._next_token_locked()
                    self.current_rv = snapshot_rv
            else:
                offset = 0
                # 快照版本若已被压缩，List 仍可服务（List 允许 quorum read），
                # 但要推进到新的有效版本，作为之后 Watch 的合法续接点。
                if self.current_rv in self.expired_tokens:
                    snapshot_rv = self._next_token_locked()
                    self.current_rv = snapshot_rv
                else:
                    snapshot_rv = self.current_rv
            items = sorted(
                (obj for obj in self.objects.values()
                 if namespace is None or obj["metadata"]["namespace"] == namespace),
                key=lambda obj: (obj["metadata"]["namespace"], obj["metadata"]["name"],
                                 obj["metadata"]["uid"]),
            )
            page = items[offset:offset + limit]
            next_offset = offset + len(page)
            next_token = ""
            if next_offset < len(items):
                next_token = _encode_continue(next_offset, snapshot_rv)
            body = {
                "kind": "PodList",
                "metadata": {"resourceVersion": snapshot_rv, "continue": next_token},
                "items": copy.deepcopy(page),
            }
        if self.page_hook is not None:
            self.page_hook(namespace, params)
        return 200, body

    def _register_watch(self, params, namespace):
        resource_version = params.get("resourceVersion", [""])[0]
        with self.state_lock:
            if resource_version and resource_version in self.expired_tokens:
                return None, 410, {
                    "kind": "Status", "code": 410, "reason": "Expired",
                    "message": "too old resource version: " + resource_version,
                }
            self.watcher_seq += 1
            watcher_id = self.watcher_seq
            watcher = {"id": watcher_id, "namespace": namespace,
                       "queue": queue.Queue(maxsize=200), "resource_version": resource_version}
            self.watchers[watcher_id] = watcher
            bookmark = {
                "type": "BOOKMARK",
                "object": {"kind": "Pod", "metadata": {"resourceVersion": self.current_rv}},
            }
            watcher["queue"].put(bookmark)
        return watcher, 200, None


class _KubeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string, *args):
        return None

    def _send(self, status, body_obj):
        body = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        split = urlsplit(self.path)
        match = _POD_PATH.match(split.path)
        if match is None:
            self._send(404, {"kind": "Status", "code": 404, "reason": "NotFound"})
            return
        namespace = match.group(1)
        params = parse_qs(split.query)
        is_watch = params.get("watch", [""])[0] == "true"
        with self.server.state_lock:
            self.server.requests.append({
                "path": split.path,
                "params": {key: values[0] for key, values in params.items()},
                "kind": "watch" if is_watch else "list",
            })
        if is_watch:
            watcher, status, error = self.server._register_watch(params, namespace)
            if error is not None:
                self._send(status, error)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json; stream=watch")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            try:
                while True:
                    event = watcher["queue"].get(timeout=5)
                    if event is None:
                        break
                    line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
                    # 标准 HTTP/1.1 分块帧，客户端用原始套接字逐行读取事件。
                    self.wfile.write(f"{len(line):X}\r\n".encode("ascii"))
                    self.wfile.write(line)
                    self.wfile.write(b"\r\n")
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError, queue.Empty):
                pass
            finally:
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except OSError:
                    pass
                with self.server.state_lock:
                    self.server.watchers.pop(watcher["id"], None)
            return
        status, body = self.server._list(params, namespace)
        self._send(status, body)


def load_kube_scenario(path=None):
    import pathlib

    if path is not None:
        scenario_path = pathlib.Path(path)
    else:
        candidates = [
            pathlib.Path(__file__).parent / "fixtures" / "kube_scenario.json",
            pathlib.Path(__file__).parents[1] / "fixtures" / "kube_scenario.json",
        ]
        scenario_path = next((candidate for candidate in candidates if candidate.is_file()), candidates[-1])
    return json.loads(scenario_path.read_text(encoding="utf-8"))
