"""Kubernetes 只读 List / Watch HTTP 客户端。

约束：
- 只发 GET，不携带任何写操作；
- resourceVersion 全程按不透明字符串处理：仅原样保存、原样回传，
  不解析数值、不比较大小、不递增、不拼接；
- Watch 以行分隔 JSON 流式读取，断线以异常形式上交给采集状态机，
  由状态机依据已保存的进度 RV 决定续接或重列举。
"""

import http.client
import json
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit


@dataclass(frozen=True)
class StreamItem:
    kind: str  # "event" | "status"
    conn_id: str
    event: dict | None = None
    status: dict | None = None
    http_status: int = 200


def list_path(scope):
    if scope == "cluster":
        return "/api/v1/pods"
    if not scope.startswith("namespace:"):
        raise ValueError("unknown scope: " + scope)
    return "/api/v1/namespaces/%s/pods" % scope.split(":", 1)[1]


def watch_path(scope):
    if scope == "cluster":
        return "/api/v1/watch/pods"
    if not scope.startswith("namespace:"):
        raise ValueError("unknown scope: " + scope)
    return "/api/v1/namespaces/%s/pods/watch" % scope.split(":", 1)[1]


class KubeClient:
    def __init__(self, base_url, timeout=3, read_timeout=0.5):
        parsed = urlsplit(base_url)
        if parsed.scheme != "http" or not parsed.netloc:
            raise ValueError("HTTP base URL required")
        self.host = parsed.hostname
        self.port = parsed.port or 80
        self.timeout = timeout
        self.read_timeout = read_timeout

    def _connection(self, read_timeout=None):
        return http.client.HTTPConnection(self.host, self.port, timeout=read_timeout or self.timeout)

    def list_page(self, scope, limit=None, continue_token="", resource_version=""):
        """请求一页 List，返回 (http_status, payload, headers)。

        首页不带 resourceVersion（请求服务端当前快照）；后续页只回传服务端
        在 continue 令牌里给出的信息。
        """
        query = {}
        if limit is not None:
            query["limit"] = str(limit)
        if continue_token:
            query["continue"] = continue_token
        if resource_version:
            query["resourceVersion"] = resource_version
        path = list_path(scope) + ("?" + urlencode(query) if query else "")
        conn = self._connection()
        try:
            conn.request("GET", path, headers={"Accept": "application/json"})
            response = conn.getresponse()
            body = response.read()
            headers = {name.lower(): value for name, value in response.getheaders()}
            return response.status, json.loads(body.decode()), headers
        finally:
            conn.close()

    def watch(self, scope, resource_version, allow_bookmarks=True):
        """打开一条 Watch，逐行产出 StreamItem 的生成器。

        初始响应即 410/403 时产出一个 kind="status" 的条目后结束；
        传输层断线以异常形式抛出，调用方不得猜测新 RV 自行续接。
        """
        query = {"resourceVersion": resource_version, "allowWatchBookmarks": "true" if allow_bookmarks else "false"}
        path = watch_path(scope) + "?" + urlencode(query)
        conn = self._connection(read_timeout=self.read_timeout)
        conn.request("GET", path, headers={"Accept": "application/json", "Connection": "keep-alive"})
        response = conn.getresponse()
        conn_id = response.getheader("X-Kubesim-Connection-Id", "")
        if response.status != 200:
            payload = json.loads(response.read().decode())
            conn.close()
            yield StreamItem(kind="status", conn_id=conn_id, status=payload, http_status=response.status)
            return
        try:
            while True:
                try:
                    line = response.readline()
                except http.client.IncompleteRead as broken:
                    raise ConnectionError("watch stream truncated") from broken
                except TimeoutError:
                    continue
                if line == b"":
                    raise ConnectionError("watch connection closed by peer")
                line = line.strip()
                if not line:
                    continue
                envelope = json.loads(line.decode())
                item_type = envelope.get("type")
                if item_type == "ERROR":
                    yield StreamItem(kind="status", conn_id=conn_id, status=envelope.get("object", {}),
                                     http_status=200)
                    return
                yield StreamItem(kind="event", conn_id=conn_id, event=envelope)
        finally:
            conn.close()
