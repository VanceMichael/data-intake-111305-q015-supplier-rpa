"""Kubernetes 资源目录采集状态机与归档查询。

设计边界（与 docs/catalog-design.md 对应）：

1. 对象身份是 (cluster, uid)。name/namespace 只是当前定位字段，同名对象
   删除后以新 UID 重建时，旧 UID 的 DELETED 事件没有能力抹掉新 UID 的对象。
2. 一次全量重列举称为一个 round（轮次）。分页期间对象写入 staged 暂存区，
   只有该轮所有覆盖范围内的分页都成功，才在单个事务里把目录切换到新轮次；
   任何一页失败则整轮 abandon，旧目录继续有效，绝不宣布半成品为全量。
3. 交接位置只取首页 List 返回的 snapshot resourceVersion，Watch 从该不透明
   令牌开始；分页期间的变更由服务端在 Watch 回放中补齐，不依赖页内排序。
4. Watch 断线用已保存的进度 RV 原样回传续接；收到 410 Expired 时不猜测任何
   更大版本号，而是发起一轮有 List 证据的新轮次并原子切换目录。
5. 每条 Watch 连接绑定轮次。轮次切换后旧连接标记 superseded，其迟到事件
   只入证不应用（fenced），当前视图不会混入旧轮次对象。
6. 403 表示采集范围受限：标记 scope=restricted 并保留证据，受限范围内的
   旧对象原样保留，不会被当成删除；coverage 查询显式展示未覆盖范围。
7. BOOKMARK 只更新进度 RV，不产生业务对象，不触发 upsert。
8. resourceVersion 只允许相等性判断与透传，禁止数值/字典序比较。
"""

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .kube_client import KubeClient

LIST_PAGE_LIMIT = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    cluster TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    note TEXT
);
CREATE TABLE IF NOT EXISTS scope_results (
    round_id TEXT NOT NULL REFERENCES rounds(round_id),
    cluster TEXT NOT NULL,
    scope TEXT NOT NULL,
    state TEXT NOT NULL,
    list_rv TEXT,
    http_status INTEGER,
    observed_at TEXT NOT NULL,
    note TEXT,
    PRIMARY KEY (round_id, scope)
);
CREATE TABLE IF NOT EXISTS list_pages (
    id INTEGER PRIMARY KEY,
    round_id TEXT NOT NULL REFERENCES rounds(round_id),
    cluster TEXT NOT NULL,
    scope TEXT NOT NULL,
    page_index INTEGER NOT NULL,
    continue_in TEXT,
    continue_out TEXT,
    snapshot_rv TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    capture_id INTEGER,
    received_at TEXT NOT NULL,
    UNIQUE (round_id, scope, page_index)
);
CREATE TABLE IF NOT EXISTS staged_objects (
    id INTEGER PRIMARY KEY,
    round_id TEXT NOT NULL REFERENCES rounds(round_id),
    cluster TEXT NOT NULL,
    scope TEXT NOT NULL,
    uid TEXT NOT NULL,
    object_json TEXT NOT NULL,
    page_id INTEGER NOT NULL REFERENCES list_pages(id),
    observed_at TEXT NOT NULL,
    UNIQUE (round_id, scope, uid)
);
CREATE TABLE IF NOT EXISTS connections (
    conn_id TEXT PRIMARY KEY,
    cluster TEXT NOT NULL,
    round_id TEXT NOT NULL REFERENCES rounds(round_id),
    scope TEXT NOT NULL,
    state TEXT NOT NULL,
    start_rv TEXT NOT NULL,
    progress_rv TEXT,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    close_note TEXT
);
CREATE TABLE IF NOT EXISTS watch_events (
    id INTEGER PRIMARY KEY,
    cluster TEXT NOT NULL,
    conn_id TEXT,
    round_id TEXT,
    scope TEXT,
    event_type TEXT NOT NULL,
    uid TEXT,
    object_rv TEXT,
    event_json TEXT NOT NULL,
    applied INTEGER NOT NULL,
    fence_reason TEXT,
    capture_id INTEGER,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
    cluster TEXT NOT NULL,
    uid TEXT NOT NULL,
    name TEXT NOT NULL,
    namespace TEXT,
    alive INTEGER NOT NULL,
    object_json TEXT NOT NULL,
    object_rv TEXT,
    origin_kind TEXT NOT NULL,
    origin_round_id TEXT,
    origin_page_id INTEGER,
    origin_event_id INTEGER,
    first_seen_at TEXT NOT NULL,
    last_changed_at TEXT NOT NULL,
    PRIMARY KEY (cluster, uid)
);
CREATE TABLE IF NOT EXISTS object_history (
    id INTEGER PRIMARY KEY,
    cluster TEXT NOT NULL,
    uid TEXT NOT NULL,
    alive INTEGER NOT NULL,
    name TEXT NOT NULL,
    namespace TEXT,
    object_rv TEXT,
    object_json TEXT NOT NULL,
    origin_kind TEXT NOT NULL,
    origin_round_id TEXT,
    origin_page_id INTEGER,
    origin_event_id INTEGER,
    changed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scope_status (
    cluster TEXT NOT NULL,
    scope TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_rv TEXT,
    active_round_id TEXT,
    restricted_reason TEXT,
    evidence_round_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (cluster, scope)
);
"""


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def object_identity(obj):
    metadata = obj.get("metadata") or {}
    uid = metadata.get("uid")
    name = metadata.get("name")
    if not uid or not name:
        raise ValueError("object metadata.uid and metadata.name are required")
    namespace = metadata.get("namespace", "")
    return str(uid), str(name), str(namespace or ""), str(metadata.get("resourceVersion", ""))


class Catalog:
    """目录持久层：轮次、暂存、连接围栏、当前视图与证据查询。"""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        with self.connection:
            yield self.connection

    # -- 轮次 ---------------------------------------------------------------

    def start_round(self, cluster, reason="initial"):
        round_id = "round-" + uuid.uuid4().hex[:12]
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO rounds VALUES (?,?,?,?,?,NULL,NULL)",
                (round_id, cluster, "running", reason, utc_now()),
            )
        return round_id

    def finish_round(self, round_id, state, note=None):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE rounds SET state=?, finished_at=?, note=? WHERE round_id=?",
                (state, utc_now(), note, round_id),
            )

    def active_round(self, cluster):
        row = self.connection.execute(
            "SELECT * FROM rounds WHERE cluster=? AND state='promoted' ORDER BY finished_at DESC, started_at DESC LIMIT 1",
            (cluster,),
        ).fetchone()
        return dict(row) if row else None

    def round_state(self, round_id):
        row = self.connection.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
        return dict(row) if row else None

    def abandon_round(self, round_id, note):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE rounds SET state='abandoned', finished_at=?, note=? WHERE round_id=?",
                (utc_now(), note, round_id),
            )
            conn.execute(
                "UPDATE connections SET state='abandoned', closed_at=?, close_note=? "
                "WHERE round_id=? AND state IN ('open','superseded')",
                (utc_now(), "round abandoned: " + note, round_id),
            )
            conn.execute(
                "UPDATE scope_results SET state='failed', note=? WHERE round_id=? AND state='listing'",
                (note, round_id),
            )

    # -- List 分页与暂存 ----------------------------------------------------

    def record_scope_result(self, round_id, cluster, scope, state, list_rv=None, http_status=None, note=None):
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO scope_results VALUES (?,?,?,?,?,?,?,?)",
                (round_id, cluster, scope, state, list_rv, http_status, utc_now(), note),
            )

    def record_list_page(self, round_id, cluster, scope, page_index, continue_in, payload, http_status, capture_id=None):
        metadata = payload.get("metadata") or {}
        snapshot_rv = str(metadata.get("resourceVersion", ""))
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO list_pages(round_id,cluster,scope,page_index,continue_in,continue_out,"
                "snapshot_rv,http_status,capture_id,received_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (round_id, cluster, scope, page_index, continue_in, metadata.get("continue", ""),
                 snapshot_rv, http_status, capture_id, utc_now()),
            )
            page_id = cursor.lastrowid
            for obj in payload.get("items", ()):
                uid, name, namespace, obj_rv = object_identity(obj)
                conn.execute(
                    "INSERT INTO staged_objects(round_id,cluster,scope,uid,object_json,page_id,observed_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (round_id, cluster, scope, uid, json.dumps(obj, ensure_ascii=False), page_id, utc_now()),
                )
        return page_id, snapshot_rv

    # -- Watch 连接与事件 ---------------------------------------------------

    def open_connection(self, cluster, round_id, scope, start_rv):
        conn_id = "local-" + uuid.uuid4().hex[:12]
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO connections(conn_id,cluster,round_id,scope,state,start_rv,progress_rv,"
                "opened_at,closed_at,close_note) VALUES (?,?,?,?,?,?,?,?,NULL,NULL)",
                (conn_id, cluster, round_id, scope, "open", start_rv, start_rv, utc_now()),
            )
        return conn_id

    def bind_connection(self, conn_id, server_conn_id):
        with self.transaction() as conn:
            conn.execute("UPDATE connections SET conn_id=? WHERE conn_id=?", (server_conn_id, conn_id))
        return server_conn_id

    def close_connection(self, conn_id, note):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE connections SET state='closed', closed_at=?, close_note=? WHERE conn_id=?",
                (utc_now(), note, conn_id),
            )

    def connection_info(self, conn_id):
        row = self.connection.execute("SELECT * FROM connections WHERE conn_id=?", (conn_id,)).fetchone()
        return dict(row) if row else None

    def supersede_round_connections(self, round_id, scopes):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE connections SET state='superseded' WHERE round_id=? AND state='open' AND scope IN (%s)"
                % ",".join("?" for _ in scopes),
                (round_id, *scopes),
            )

    def _record_event(self, conn, cluster, conn_id, round_id, scope, event, applied, fence_reason, capture_id):
        event_type = event.get("type", "")
        obj = event.get("object") or {}
        metadata = obj.get("metadata") or {}
        cursor = conn.execute(
            "INSERT INTO watch_events(cluster,conn_id,round_id,scope,event_type,uid,object_rv,event_json,"
            "applied,fence_reason,capture_id,received_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (cluster, conn_id, round_id, scope, event_type, metadata.get("uid"),
             metadata.get("resourceVersion"), json.dumps(event, ensure_ascii=False),
             1 if applied else 0, fence_reason, capture_id, utc_now()),
        )
        return cursor.lastrowid

    def record_fenced_event(self, cluster, conn_id, event, reason, capture_id=None):
        info = self.connection_info(conn_id) or {}
        with self.transaction() as conn:
            self._record_event(conn, cluster, conn_id, info.get("round_id"), info.get("scope"),
                               event, False, reason, capture_id)

    def record_progress(self, conn_id, progress_rv):
        with self.transaction() as conn:
            conn.execute("UPDATE connections SET progress_rv=? WHERE conn_id=?", (progress_rv, conn_id))
            row = conn.execute("SELECT cluster,scope FROM connections WHERE conn_id=?", (conn_id,)).fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE scope_status SET progress_rv=?, updated_at=? WHERE cluster=? AND scope=?",
                    (progress_rv, utc_now(), row["cluster"], row["scope"]),
                )

    # -- 轮次切换（原子目录交换）--------------------------------------------

    def promote_round(self, round_id, cluster, scopes, restricted_scopes):
        """把暂存区提升为当前视图，单事务完成。

        - 覆盖范围内：以新快照为准，快照中缺失的存活 UID 标记删除；
        - 受限范围：完全不参与对账，旧对象原样保留；
        - 旧轮次的 Watch 连接统一标记 superseded，迟到事件将被围栏。
        """
        with self.transaction() as conn:
            row = conn.execute("SELECT state FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None or row["state"] != "running":
                raise ValueError("round is not promotable: " + round_id)
            for scope in scopes:
                result = conn.execute(
                    "SELECT state FROM scope_results WHERE round_id=? AND scope=?", (round_id, scope)
                ).fetchone()
                if scope in restricted_scopes:
                    continue
                if result is None or result["state"] != "listed":
                    raise ValueError("scope %s has no complete list evidence in round %s" % (scope, round_id))

            staged = conn.execute(
                "SELECT * FROM staged_objects WHERE round_id=? AND scope IN (%s)"
                % ",".join("?" for _ in scopes),
                (round_id, *scopes),
            ).fetchall()
            present = {}
            for item in staged:
                obj = json.loads(item["object_json"])
                uid, name, namespace, obj_rv = object_identity(obj)
                present[(cluster, uid)] = (obj, name, namespace, obj_rv, item)

            for (stored_cluster, uid), (obj, name, namespace, obj_rv, item) in present.items():
                existing = conn.execute(
                    "SELECT object_rv FROM objects WHERE cluster=? AND uid=?", (cluster, uid)
                ).fetchone()
                if existing is not None and existing["object_rv"] == obj_rv:
                    continue
                changed_at = utc_now()
                conn.execute(
                    "INSERT INTO objects(cluster,uid,name,namespace,alive,object_json,object_rv,origin_kind,"
                    "origin_round_id,origin_page_id,origin_event_id,first_seen_at,last_changed_at) "
                    "VALUES (?,?,?,?,1,?,?, 'list', ?, ?, NULL, ?, ?) "
                    "ON CONFLICT(cluster,uid) DO UPDATE SET name=excluded.name, namespace=excluded.namespace,"
                    "alive=1, object_json=excluded.object_json, object_rv=excluded.object_rv,"
                    "origin_kind='list', origin_round_id=excluded.origin_round_id,"
                    "origin_page_id=excluded.origin_page_id, origin_event_id=NULL, last_changed_at=excluded.last_changed_at",
                    (cluster, uid, name, namespace, json.dumps(obj, ensure_ascii=False), obj_rv,
                     round_id, item["page_id"], changed_at, changed_at),
                )
                first = conn.execute(
                    "SELECT first_seen_at FROM objects WHERE cluster=? AND uid=?", (cluster, uid)
                ).fetchone()
                first_seen = first["first_seen_at"] if first is not None else changed_at
                conn.execute(
                    "INSERT INTO object_history(cluster,uid,alive,name,namespace,object_rv,object_json,"
                    "origin_kind,origin_round_id,origin_page_id,origin_event_id,changed_at)"
                    " VALUES (?,?,1,?,?,?,?,'list',?,?,NULL,?)",
                    (cluster, uid, name, namespace, obj_rv, json.dumps(obj, ensure_ascii=False),
                     round_id, item["page_id"], changed_at),
                )

            covered = [scope for scope in scopes if scope not in restricted_scopes]
            covered_namespaces = [s.split(":", 1)[1] for s in covered if s.startswith("namespace:")]
            if "cluster" in covered:
                location_clause = "1=1"
                location_params = []
            else:
                location_clause = "namespace IN (%s)" % ",".join("?" for _ in covered_namespaces)
                location_params = covered_namespaces
            present_clause = "scope IN (%s)" % ",".join("?" for _ in covered)
            missing = conn.execute(
                "SELECT * FROM objects WHERE cluster=? AND alive=1 AND " + location_clause +
                " AND uid NOT IN (SELECT uid FROM staged_objects WHERE round_id=? AND " + present_clause + ")",
                (cluster, *location_params, round_id, *covered),
            ).fetchall()
            for item in missing:
                changed_at = utc_now()
                conn.execute("UPDATE objects SET alive=0, last_changed_at=? WHERE cluster=? AND uid=?",
                             (changed_at, cluster, item["uid"]))
                conn.execute(
                    "INSERT INTO object_history(cluster,uid,alive,name,namespace,object_rv,object_json,"
                    "origin_kind,origin_round_id,origin_page_id,origin_event_id,changed_at)"
                    " VALUES (?,?,0,?,?,?,?, 'reconcile', ?,NULL,NULL,?)",
                    (cluster, item["uid"], item["name"], item["namespace"], item["object_rv"],
                     item["object_json"], round_id, changed_at),
                )

            conn.execute("UPDATE rounds SET state='promoted', finished_at=? WHERE round_id=?",
                         (utc_now(), round_id))
            previous = conn.execute(
                "SELECT round_id FROM rounds WHERE cluster=? AND state='promoted' AND round_id<>? LIMIT 1",
                (cluster, round_id),
            ).fetchall()
            for old in previous:
                conn.execute(
                    "UPDATE connections SET state='superseded' WHERE round_id=? AND state='open'",
                    (old["round_id"],),
                )
            for scope in scopes:
                result = conn.execute(
                    "SELECT state,list_rv FROM scope_results WHERE round_id=? AND scope=?",
                    (round_id, scope),
                ).fetchone()
                restricted = scope in restricted_scopes
                conn.execute(
                    "INSERT INTO scope_status(cluster,scope,state,progress_rv,active_round_id,"
                    "restricted_reason,evidence_round_id,updated_at) VALUES (?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(cluster,scope) DO UPDATE SET state=excluded.state,"
                    "progress_rv=excluded.progress_rv, active_round_id=excluded.active_round_id,"
                    " restricted_reason=excluded.restricted_reason,"
                    "evidence_round_id=excluded.evidence_round_id, updated_at=excluded.updated_at",
                    (cluster, scope, "restricted" if restricted else "watching",
                     result["list_rv"] if result else None, round_id,
                     "HTTP 403: read grant does not cover scope" if restricted else None,
                     round_id, utc_now()),
                )

    def apply_watch_event(self, conn_id, event, capture_id=None):
        """应用一条当前连接的 Watch 事件，返回 (applied, reason)。"""
        info = self.connection_info(conn_id)
        if info is None:
            return False, "unknown_connection"
        cluster, round_id, scope = info["cluster"], info["round_id"], info["scope"]
        event_type = event.get("type", "")
        obj = event.get("object") or {}
        metadata = obj.get("metadata") or {}
        obj_rv = str(metadata.get("resourceVersion", ""))

        with self.transaction() as conn:
            connection = conn.execute("SELECT * FROM connections WHERE conn_id=?", (conn_id,)).fetchone()
            if connection is None:
                reason = "unknown_connection"
                self._record_event(conn, cluster, conn_id, round_id, scope, event, False, reason, capture_id)
                return False, reason
            if connection["state"] != "open":
                reason = "connection_" + connection["state"]
                self._record_event(conn, cluster, conn_id, round_id, scope, event, False, reason, capture_id)
                return False, reason
            round_row = conn.execute("SELECT state FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if round_row is None or round_row["state"] != "promoted":
                reason = "round_not_promoted"
                self._record_event(conn, cluster, conn_id, round_id, scope, event, False, reason, capture_id)
                return False, reason

            if event_type == "BOOKMARK":
                if obj_rv:
                    conn.execute("UPDATE connections SET progress_rv=? WHERE conn_id=?", (obj_rv, conn_id))
                    conn.execute(
                        "UPDATE scope_status SET progress_rv=?, updated_at=? WHERE cluster=? AND scope=?",
                        (obj_rv, utc_now(), cluster, scope),
                    )
                self._record_event(conn, cluster, conn_id, round_id, scope, event, True, None, capture_id)
                return True, None

            uid, name, namespace, _ = object_identity(obj)
            existing = conn.execute("SELECT * FROM objects WHERE cluster=? AND uid=?", (cluster, uid)).fetchone()
            if event_type in ("ADDED", "MODIFIED"):
                if existing is not None and existing["object_rv"] == obj_rv:
                    self._record_event(conn, cluster, conn_id, round_id, scope, event, False,
                                       "duplicate_rv", capture_id)
                    return False, "duplicate_rv"
                changed_at = utc_now()
                conn.execute(
                    "INSERT INTO objects(cluster,uid,name,namespace,alive,object_json,object_rv,origin_kind,"
                    "origin_round_id,origin_page_id,origin_event_id,first_seen_at,last_changed_at) "
                    "VALUES (?,?,?,?,1,?,?,'watch',?,NULL,?,?,?) "
                    "ON CONFLICT(cluster,uid) DO UPDATE SET name=excluded.name, namespace=excluded.namespace,"
                    "alive=1, object_json=excluded.object_json, object_rv=excluded.object_rv,"
                    "origin_kind='watch', origin_round_id=excluded.origin_round_id, origin_page_id=NULL,"
                    "origin_event_id=excluded.origin_event_id, last_changed_at=excluded.last_changed_at",
                    (cluster, uid, name, namespace, json.dumps(obj, ensure_ascii=False), obj_rv,
                     round_id, changed_at, changed_at, changed_at),
                )
                event_id = self._record_event(conn, cluster, conn_id, round_id, scope, event, True, None, capture_id)
                conn.execute("UPDATE objects SET origin_event_id=? WHERE cluster=? AND uid=?",
                             (event_id, cluster, uid))
                conn.execute(
                    "INSERT INTO object_history(cluster,uid,alive,name,namespace,object_rv,object_json,"
                    "origin_kind,origin_round_id,origin_page_id,origin_event_id,changed_at)"
                    " VALUES (?,?,1,?,?,?,?,'watch',?,NULL,?,?)",
                    (cluster, uid, name, namespace, obj_rv, json.dumps(obj, ensure_ascii=False),
                     round_id, event_id, changed_at),
                )
                conn.execute(
                    "UPDATE scope_status SET state='watching', updated_at=? WHERE cluster=? AND scope=?",
                    (utc_now(), cluster, scope),
                )
                return True, None

            if event_type == "DELETED":
                # 身份按 UID：新 UID 的同名对象不受旧 UID 删除事件影响。
                if existing is None:
                    self._record_event(conn, cluster, conn_id, round_id, scope, event, False,
                                       "uid_unknown", capture_id)
                    return False, "uid_unknown"
                if existing["object_rv"] == obj_rv and existing["alive"] == 0:
                    self._record_event(conn, cluster, conn_id, round_id, scope, event, False,
                                       "duplicate_rv", capture_id)
                    return False, "duplicate_rv"
                changed_at = utc_now()
                conn.execute(
                    "UPDATE objects SET alive=0, object_json=?, object_rv=?, origin_kind='watch',"
                    "origin_round_id=?, origin_page_id=NULL, last_changed_at=? WHERE cluster=? AND uid=?",
                    (json.dumps(obj, ensure_ascii=False), obj_rv, round_id, changed_at, cluster, uid),
                )
                event_id = self._record_event(conn, cluster, conn_id, round_id, scope, event, True, None, capture_id)
                conn.execute("UPDATE objects SET origin_event_id=? WHERE cluster=? AND uid=?",
                             (event_id, cluster, uid))
                conn.execute(
                    "INSERT INTO object_history(cluster,uid,alive,name,namespace,object_rv,object_json,"
                    "origin_kind,origin_round_id,origin_page_id,origin_event_id,changed_at)"
                    " VALUES (?,?,0,?,?,?,?,'watch',?,NULL,?,?)",
                    (cluster, uid, name, namespace, obj_rv, json.dumps(obj, ensure_ascii=False),
                     round_id, event_id, changed_at),
                )
                return True, None

            self._record_event(conn, cluster, conn_id, round_id, scope, event, False,
                               "unsupported_type", capture_id)
            return False, "unsupported_type"

    def mark_scope_restricted(self, cluster, scope, round_id, http_status, note):
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO scope_results VALUES (?,?,?,?,?,?,?,?)",
                (round_id, cluster, scope, "restricted", None, http_status, utc_now(), note),
            )
            conn.execute(
                "INSERT INTO scope_status(cluster,scope,state,progress_rv,active_round_id,"
                "restricted_reason,evidence_round_id,updated_at) VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(cluster,scope) DO UPDATE SET state='restricted',"
                "restricted_reason=excluded.restricted_reason, evidence_round_id=excluded.evidence_round_id,"
                "updated_at=excluded.updated_at",
                (cluster, scope, "restricted", None, round_id, note, round_id, utc_now()),
            )

    def resume_state(self, cluster):
        """重启后恢复：返回 (round_id, {scope: progress_rv})，无活动轮次则 None。"""
        active = self.active_round(cluster)
        if active is None:
            return None
        rows = self.connection.execute(
            "SELECT scope, progress_rv FROM scope_status WHERE cluster=? AND state='watching'", (cluster,)
        ).fetchall()
        return active["round_id"], {row["scope"]: row["progress_rv"] for row in rows}

    # -- 查询：归档人员视角 -------------------------------------------------

    def objects(self, cluster, alive_only=True):
        query = "SELECT * FROM objects WHERE cluster=?"
        if alive_only:
            query += " AND alive=1"
        query += " ORDER BY namespace,name,uid"
        return [dict(row) for row in self.connection.execute(query, (cluster,)).fetchall()]

    def object_provenance(self, cluster, uid):
        row = self.connection.execute("SELECT * FROM objects WHERE cluster=? AND uid=?", (cluster, uid)).fetchone()
        if row is None:
            raise KeyError(uid)
        history = self.connection.execute(
            "SELECT * FROM object_history WHERE cluster=? AND uid=? ORDER BY id", (cluster, uid)
        ).fetchall()
        return {"current": dict(row), "history": [dict(item) for item in history]}

    def round_evidence(self, round_id):
        pages = self.connection.execute(
            "SELECT * FROM list_pages WHERE round_id=? ORDER BY scope,page_index", (round_id,)
        ).fetchall()
        results = self.connection.execute(
            "SELECT * FROM scope_results WHERE round_id=? ORDER BY scope", (round_id,)
        ).fetchall()
        events = self.connection.execute(
            "SELECT * FROM watch_events WHERE round_id=? ORDER BY id", (round_id,)
        ).fetchall()
        return {
            "round": self.round_state(round_id),
            "scope_results": [dict(row) for row in results],
            "list_pages": [dict(row) for row in pages],
            "watch_events": [dict(row) for row in events],
        }

    def coverage(self, cluster, configured_scopes):
        rows = self.connection.execute("SELECT * FROM scope_status WHERE cluster=?", (cluster,)).fetchall()
        known = {row["scope"]: dict(row) for row in rows}
        result = []
        for scope in configured_scopes:
            entry = known.get(scope)
            if entry is None:
                result.append({"scope": scope, "state": "never_covered", "evidence": None,
                               "note": "no list or watch evidence has ever been recorded"})
                continue
            result.append({
                "scope": scope,
                "state": entry["state"],
                "active_round_id": entry["active_round_id"],
                "progress_rv": entry["progress_rv"],
                "restricted_reason": entry["restricted_reason"],
                "evidence_round_id": entry["evidence_round_id"],
            })
        return result

    def fence_stats(self, cluster):
        rows = self.connection.execute(
            "SELECT fence_reason, count(*) AS n FROM watch_events WHERE cluster=? AND applied=0 "
            "GROUP BY fence_reason ORDER BY fence_reason",
            (cluster,),
        ).fetchall()
        return {row["fence_reason"]: row["n"] for row in rows}


@dataclass
class _Stream:
    scope: str
    generator: object
    conn_id: str
    start_rv: str


class Ingestor:
    """驱动 List/Watch 采集循环；只发 GET，不执行任何写操作。"""

    def __init__(self, client, catalog, cluster, scopes, archive=None, source=None, page_limit=LIST_PAGE_LIMIT):
        self.client = client
        self.catalog = catalog
        self.cluster = cluster
        self.scopes = list(scopes)
        self.archive = archive
        self.source = source or ("kubesim:" + cluster)
        self.page_limit = page_limit
        self.streams = {}
        # 轮次切换后保留旧连接读取器：继续排空其队列，事件只入证不应用。
        self.superseded_streams = {}
        self.stop = threading.Event()
        self.relist_pending = False
        self.relist_reason = ""
        self._round_id = None

    @property
    def round_id(self):
        return self._round_id

    def _archive(self, url_path, payload):
        if self.archive is None:
            return None
        body = json.dumps(payload, ensure_ascii=False).encode()
        return self.archive.append(self.source, url_path, 200, {"content-type": "application/json"}, body)

    # -- 全量列举 -----------------------------------------------------------

    def synchronize(self, reason="initial"):
        round_id = self.catalog.start_round(self.cluster, reason)
        restricted = set()
        try:
            for scope in self.scopes:
                self.catalog.record_scope_result(round_id, self.cluster, scope, "listing")
                continue_token = ""
                page_index = 0
                snapshot_rv = None
                while True:
                    status, payload, _headers = self.client.list_page(
                        scope, limit=self.page_limit, continue_token=continue_token
                    )
                    if status == 403:
                        note = (payload.get("message") or "HTTP 403 Forbidden")
                        self.catalog.mark_scope_restricted(self.cluster, scope, round_id, status, note)
                        restricted.add(scope)
                        break
                    if status != 200:
                        raise RuntimeError("list failed for %s with HTTP %s: %s"
                                           % (scope, status, json.dumps(payload, ensure_ascii=False)))
                    capture_id = self._archive("/list/" + scope, payload)
                    page_id, page_rv = self.catalog.record_list_page(
                        round_id, self.cluster, scope, page_index, continue_token, payload, status, capture_id
                    )
                    if page_index == 0:
                        snapshot_rv = page_rv
                        if not snapshot_rv:
                            raise RuntimeError("first list page carried no resourceVersion")
                    elif page_rv != snapshot_rv:
                        # 分页快照不一致时不能拼接成全量，整轮废弃。
                        raise RuntimeError("paged list resourceVersion mismatch on scope %s" % scope)
                    next_token = (payload.get("metadata") or {}).get("continue", "")
                    if not next_token:
                        break
                    continue_token = next_token
                    page_index += 1
                if scope not in restricted:
                    self.catalog.record_scope_result(round_id, self.cluster, scope, "listed",
                                                     list_rv=snapshot_rv, http_status=200)
            self.catalog.promote_round(round_id, self.cluster, self.scopes, restricted)
        except Exception as problem:
            self.catalog.abandon_round(round_id, str(problem))
            raise
        self._round_id = round_id
        self.relist_pending = False
        self.relist_reason = ""
        # 旧连接移到围栏区继续排空；新轮次稍后惰性建立全新连接集合。
        for stream in list(self.streams.values()):
            self.superseded_streams[stream.conn_id] = stream
        self.streams = {}
        return round_id

    # -- Watch 连接管理 -----------------------------------------------------

    def _ensure_stream(self, scope):
        if scope in self.streams:
            return
        status_row = self.catalog.connection.execute(
            "SELECT state, progress_rv FROM scope_status WHERE cluster=? AND scope=?",
            (self.cluster, scope),
        ).fetchone()
        if status_row is not None and status_row["state"] == "restricted":
            return
        start_rv = status_row["progress_rv"] if status_row is not None else None
        if not start_rv:
            result = self.catalog.connection.execute(
                "SELECT list_rv FROM scope_results WHERE round_id=? AND scope=?",
                (self._round_id, scope),
            ).fetchone()
            start_rv = result["list_rv"] if result else None
        if not start_rv:
            raise RuntimeError("no evidence-based resourceVersion available for watch on scope " + scope)
        local_id = self.catalog.open_connection(self.cluster, self._round_id, scope, start_rv)
        generator = self.client.watch(scope, start_rv)
        self.streams[scope] = _Stream(scope, generator, local_id, start_rv)

    def _drop_stream(self, scope, note):
        stream = self.streams.pop(scope, None)
        if stream is not None:
            self.catalog.close_connection(stream.conn_id, note)

    # -- 单步抽取，便于测试与事件循环复用 -----------------------------------

    def _drain_superseded(self):
        """排空旧轮次连接：所有事件记录为围栏证据，绝不进入当前视图。"""
        for conn_id, stream in list(self.superseded_streams.items()):
            try:
                item = next(stream.generator)
            except StopIteration:
                del self.superseded_streams[conn_id]
                continue
            except (ConnectionError, OSError) as broken:
                del self.superseded_streams[conn_id]
                self.catalog.close_connection(conn_id, "old connection lost: " + str(broken))
                continue
            if item.kind == "status":
                del self.superseded_streams[conn_id]
                self.catalog.close_connection(conn_id, "old connection status %s"
                                              % ((item.status or {}).get("code"),))
                continue
            # BOOKMARK 只表示进度，不是业务对象，旧连接的心跳直接丢弃。
            if item.event.get("type") == "BOOKMARK":
                continue
            capture_id = self._archive("/watch/stale/" + stream.scope, item.event)
            self.catalog.record_fenced_event(self.cluster, conn_id, item.event,
                                             "stale_round_connection", capture_id)

    def pump_once(self):
        self._drain_superseded()
        for scope in list(self.streams) + [s for s in self.scopes if s not in self.streams]:
            if self.relist_pending:
                return
            self._ensure_stream(scope)
            stream = self.streams.get(scope)
            if stream is None:
                continue
            try:
                item = next(stream.generator)
            except StopIteration:
                self._drop_stream(scope, "stream ended")
                continue
            except (ConnectionError, OSError) as broken:
                self._drop_stream(scope, "transport error: " + str(broken))
                continue
            if not stream.conn_id.startswith("conn-") and item.conn_id:
                stream.conn_id = self.catalog.bind_connection(stream.conn_id, item.conn_id)
            if item.kind == "status":
                code = (item.status or {}).get("code") or item.http_status
                if code == 410:
                    # 关键：不推进 RV、不猜测版本，交给有证据的重新列举。
                    self.relist_pending = True
                    self.relist_reason = "watch 410 Expired on scope " + scope
                    self._drop_stream(scope, "410 expired; relist required")
                    return
                if code == 403:
                    note = (item.status or {}).get("message") or "HTTP 403 on watch"
                    self.catalog.mark_scope_restricted(self.cluster, scope, self._round_id, code, note)
                    self._drop_stream(scope, "403 forbidden; scope marked restricted")
                    continue
                self._drop_stream(scope, "watch error status %s" % code)
                continue
            capture_id = self._archive("/watch/" + scope, item.event)
            info = self.catalog.connection_info(stream.conn_id)
            if info is None:
                self.catalog.record_fenced_event(self.cluster, stream.conn_id, item.event,
                                                 "unknown_connection", capture_id)
                continue
            if info["state"] != "open" or info["round_id"] != self._round_id:
                self.catalog.record_fenced_event(self.cluster, stream.conn_id, item.event,
                                                 "stale_round_connection", capture_id)
                continue
            applied, reason = self.catalog.apply_watch_event(stream.conn_id, item.event, capture_id)
            if applied and item.event.get("type") != "BOOKMARK":
                obj_rv = str((item.event.get("object") or {}).get("metadata", {}).get("resourceVersion", ""))
                if obj_rv:
                    self.catalog.record_progress(stream.conn_id, obj_rv)

    def run(self, max_pumps=None, idle_sleep=0.01):
        if self._round_id is None:
            self.synchronize("initial")
        pumps = 0
        while not self.stop.is_set():
            if self.relist_pending:
                reason = self.relist_reason or "relist"
                self.synchronize(reason)
            self.pump_once()
            pumps += 1
            if max_pumps is not None and pumps >= max_pumps:
                break
            if idle_sleep:
                time.sleep(idle_sleep)

    def resume(self):
        """进程重启后从已持久化的活动轮次恢复 Watch，不重新列举。"""
        state = self.catalog.resume_state(self.cluster)
        if state is None:
            return self.synchronize("initial")
        round_id, progress = state
        self._round_id = round_id
        self.streams = {}
        for scope, rv in progress.items():
            if rv:
                local_id = self.catalog.open_connection(self.cluster, round_id, scope, rv)
                self.streams[scope] = _Stream(scope, self.client.watch(scope, rv), local_id, rv)
        return round_id

    def shutdown(self):
        self.stop.set()
        for scope in list(self.streams):
            self._drop_stream(scope, "ingestor shutdown")
        for conn_id, stream in list(self.superseded_streams.items()):
            self.catalog.close_connection(conn_id, "ingestor shutdown")
        self.superseded_streams.clear()
