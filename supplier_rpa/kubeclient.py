"""Kubernetes 只读 List / Watch HTTP 客户端。

客户端对 resourceVersion 完全不透明：只原样保存、原样回传，从不解析
其数值、字典序或做任何大小比较。分页只跟随服务端给的 ``continue``
令牌，不自行推断偏移。Watch 把 410 作为显式异常上抛，由目录层决定
整轮重列举。
"""

import http.client
import json
import socket
from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen


class WatchExpired(Exception):
    """服务端返回 410 Gone：旧 resourceVersion 已不可恢复。"""

    def __init__(self, status_body):
        super().__init__("watch resource version expired (410)")
        self.status_body = status_body


class WatchDisconnected(Exception):
    """Watch 连接非 410 中断，可凭最后进度 token 续接。"""


@dataclass(frozen=True)
class ListPage:
    url: str
    status: int
    body: bytes
    payload: dict
    resource_version: str
    continue_token: str
    capture_id: int


def _pods_path(namespace):
    if namespace:
        return f"/api/v1/namespaces/{namespace}/pods"
    return "/api/v1/pods"


class KubeClient:
    def __init__(self, base_url, timeout=5, page_limit=500, watch_idle_timeout=20):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("HTTP base URL required")
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.page_limit = page_limit
        # Watch 流上两次报文之间允许的最大静默；模拟器事件稀疏时不会被误判断连。
        self.watch_idle_timeout = watch_idle_timeout

    def _url(self, path, query):
        return urljoin(self.base_url, path + ("?" + urlencode(query) if query else ""))

    def list_page(self, archive, source, namespace=None, continue_token=""):
        """请求一页 List，并把整页原始响应追加到归档。"""
        query = {"limit": str(self.page_limit)}
        if continue_token:
            query["continue"] = continue_token
        url = self._url(_pods_path(namespace), query)
        request = Request(url)
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        with response:
            status = response.status
            headers = {name.lower(): value for name, value in response.headers.items()}
            body = response.read()
        capture_id = archive.append(source, url, status, headers, body)
        payload = json.loads(body)
        return ListPage(
            url=url, status=status, body=body, payload=payload,
            resource_version=str(payload.get("metadata", {}).get("resourceVersion", "")),
            continue_token=payload.get("metadata", {}).get("continue", "") or "",
            capture_id=capture_id,
        )

    def open_watch(self, archive, source, namespace=None, resource_version=""):
        """建立一条 Watch 流。

        返回可逐行迭代的 :class:`WatchStream`。410 立即抛出
        :class:`WatchExpired`，不进入流式状态。这里直接使用
        :mod:`http.client`，在拿到状态行与响应头后接管原始套接字，
        避免高层响应对象把首个分块事件吞进它自己的读取缓冲。
        """
        query = {"watch": "true"}
        if resource_version:
            query["resourceVersion"] = resource_version
        path = _pods_path(namespace) + "?" + urlencode(query)
        url = urljoin(self.base_url, path)
        parsed = urlsplit(url)
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port,
                                                timeout=self.timeout)
        try:
            connection.request("GET", path, headers={"Accept": "application/json"})
            response = connection.getresponse()
        except Exception:
            connection.close()
            raise
        status = response.status
        headers = {name.lower(): value for name, value in response.getheaders()}
        if status != 200:
            body = response.read()
            connection.close()
            archive.append(source, url, status, headers, body)
            if status == 410:
                raise WatchExpired(json.loads(body))
            raise WatchDisconnected(f"watch rejected with status {status}")
        # 状态行/头部之外，response 未消费数据；其底层套接字上的后续字节
        # 就是分块事件流。设置静默超时后接管该套接字。
        sock = response.fp.raw._sock
        sock.settimeout(self.watch_idle_timeout)
        return WatchStream(connection, response, sock, url)


class WatchStream:
    """NDJSON Watch 流；BOOKMARK 与业务事件原样逐条产出（纯网络层）。"""

    def __init__(self, connection, response, sock, url):
        self.connection = connection
        self.response = response
        self.sock = sock
        self.url = url
        self.closed = False
        self._buffer = b""

    def __iter__(self):
        return self

    def _readline(self):
        # 服务端以 HTTP/1.1 分块帧承载 NDJSON：
        # 每个事件形如 "<hex 长度>\r\n<事件 JSON>\n\r\n"。
        # 兼容模拟器在连接建立首包内可能不加分块封装的情况；解析始终
        # 基于缓冲区状态，不依赖一次 recv 的包边界。
        while True:
            if not self._buffer:
                chunk = self.sock.recv(65536)
                if not chunk:
                    return b""
                self._buffer += chunk
            # 跳过帧间 CRLF。
            if self._buffer.startswith(b"\r\n"):
                self._buffer = self._buffer[2:]
                continue
            # 容错：首包直接是 NDJSON（无分块大小行）。
            if self._buffer[:1] in (b"{", b"["):
                if b"\n" not in self._buffer:
                    more = self.sock.recv(65536)
                    if not more:
                        line, self._buffer = self._buffer, b""
                        return line.strip()
                    self._buffer += more
                    continue
                line, self._buffer = self._buffer.split(b"\n", 1)
                return line.strip()
            if b"\r\n" not in self._buffer:
                more = self.sock.recv(65536)
                if not more:
                    line, self._buffer = self._buffer, b""
                    return line.strip()
                self._buffer += more
                continue
            size_line, rest = self._buffer.split(b"\r\n", 1)
            try:
                size = int(size_line.strip(), 16)
            except ValueError:
                # 非分块帧：按普通 NDJSON 行处理。
                line, self._buffer = self._buffer.split(b"\n", 1)
                return line.strip()
            if size == 0:
                self._buffer = b""
                return b""
            while len(rest) < size + 2:
                more = self.sock.recv(65536)
                if not more:
                    break
                rest += more
            payload = rest[:size]
            self._buffer = rest[size + 2:]  # 跳过数据后的 CRLF
            return payload.rstrip(b"\r\n")

    def __next__(self):
        try:
            line = self._readline()
        except (socket.timeout, TimeoutError) as error:
            self.close()
            raise WatchDisconnected(str(error)) from error
        except (ConnectionError, OSError) as error:
            self.close()
            raise WatchDisconnected(str(error)) from error
        if not line:
            self.close()
            raise WatchDisconnected("stream ended without status object")
        stripped = line.strip()
        if not stripped:
            return self.__next__()
        return json.loads(stripped)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.connection.close()
        except OSError:
            pass
