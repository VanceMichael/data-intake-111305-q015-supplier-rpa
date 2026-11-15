"""资源目录的持久化状态。

与只追加的原始 :class:`~supplier_rpa.archive.Archive` 不同，本模块保存
目录层解释出来的当前视图与证据链。所有写操作都在 SQLite 事务内完成，
进程重启后可凭 ``kv`` 中的检查点继续，旧轮次对象不会混入当前视图。

身份约定：

- 对象的稳定身份是 ``(cluster, uid)``；name / namespace 只是当前定位，
  可以被后续观察更新，删除严格按 UID 命中；
- resourceVersion 一律按不透明字符串处理，只做相等比较和原样回传；
- 每一轮重列举有独立 round，Watch 事件与连接都绑定 round，晋升时旧
  round 的连接全部栅栏化，迟到事件只能留在审计表里。
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

# 事件类型常量
EVT_ADDED = "ADDED"
EVT_MODIFIED = "MODIFIED"
EVT_DELETED = "DELETED"
EVT_BOOKMARK = "BOOKMARK"

# 证据来源
ORIGIN_LIST = "list"
ORIGIN_WATCH = "watch"

# scope 覆盖状态
COV_COVERED = "covered"
COV_RESTRICTED = "restricted"


class Catalog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rounds (
                round_id INTEGER PRIMARY KEY,
                cluster TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                promoted_at TEXT,
                status TEXT NOT NULL,
                snapshot_rv TEXT,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS list_pages (
                id INTEGER PRIMARY KEY,
                round_id INTEGER NOT NULL REFERENCES rounds(round_id),
                cluster TEXT NOT NULL,
                scope TEXT NOT NULL,
                page_index INTEGER NOT NULL,
                continue_token TEXT NOT NULL,
                resource_version TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                capture_id INTEGER,
                observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS watch_connections (
                connection_id INTEGER PRIMARY KEY,
                round_id INTEGER NOT NULL REFERENCES rounds(round_id),
                cluster TEXT NOT NULL,
                scope TEXT,
                resource_version TEXT NOT NULL,
                active INTEGER NOT NULL,
                ended_reason TEXT,
                opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS staged_events (
                id INTEGER PRIMARY KEY,
                round_id INTEGER NOT NULL,
                cluster TEXT NOT NULL,
                connection_id INTEGER REFERENCES watch_connections(connection_id),
                event_type TEXT NOT NULL,
                uid TEXT,
                name TEXT,
                namespace TEXT,
                resource_version TEXT,
                capture_id INTEGER,
                payload TEXT NOT NULL,
                applied INTEGER NOT NULL DEFAULT 0,
                stale INTEGER NOT NULL DEFAULT 0,
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS objects (
                cluster TEXT NOT NULL,
                uid TEXT NOT NULL,
                name TEXT NOT NULL,
                namespace TEXT NOT NULL,
                resource_version TEXT NOT NULL,
                state TEXT NOT NULL,
                first_seen_round INTEGER NOT NULL,
                last_event_round INTEGER NOT NULL,
                last_origin TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                payload TEXT NOT NULL,
                PRIMARY KEY (cluster, uid)
            );
            CREATE TABLE IF NOT EXISTS object_provenance (
                id INTEGER PRIMARY KEY,
                cluster TEXT NOT NULL,
                uid TEXT NOT NULL,
                origin TEXT NOT NULL,
                round_id INTEGER NOT NULL,
                page_id INTEGER,
                staged_event_id INTEGER,
                connection_id INTEGER,
                capture_id INTEGER,
                event_type TEXT,
                resource_version TEXT,
                observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS scope_coverage (
                id INTEGER PRIMARY KEY,
                cluster TEXT NOT NULL,
                scope TEXT NOT NULL,
                round_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                evidence_capture_id INTEGER,
                observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE (cluster, scope, round_id)
            );
            CREATE INDEX IF NOT EXISTS idx_staged_round ON staged_events(round_id, id);
            CREATE INDEX IF NOT EXISTS idx_objects_lookup ON objects(cluster, namespace, name);
            CREATE INDEX IF NOT EXISTS idx_provenance_object ON object_provenance(cluster, uid, id);
            """
        )

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        with self.connection:
            yield self.connection

    # ---- 检查点（不透明 token，原样存取，禁止数值化） ----

    def put_kv(self, key, value):
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def get_kv(self, key):
        row = self.connection.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row is not None else None

    def resume_token(self, cluster):
        return self.get_kv(f"resume_token:{cluster}")

    def active_round(self, cluster):
        value = self.get_kv(f"active_round:{cluster}")
        return int(value) if value is not None else None

    # ---- 轮次与分页 ----

    def begin_round(self, cluster, note=""):
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO rounds(cluster,status,note) VALUES(?,'in_progress',?)",
                (cluster, note),
            )
        return cursor.lastrowid

    def record_page(self, round_id, cluster, scope, page_index, continue_token,
                    resource_version, status_code, capture_id):
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO list_pages(round_id,cluster,scope,page_index,continue_token,"
                "resource_version,status_code,capture_id) VALUES(?,?,?,?,?,?,?,?)",
                (round_id, cluster, scope, page_index, continue_token,
                 resource_version, status_code, capture_id),
            )
        return cursor.lastrowid

    def fail_round(self, round_id, reason):
        """整组分页未完成或校验失败：轮次作废，绝不晋升为全量。"""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE rounds SET status='failed', note=? WHERE round_id=?",
                (reason, round_id),
            )
            conn.execute(
                "UPDATE staged_events SET stale=1 WHERE round_id=? AND applied=0",
                (round_id,),
            )

    def round_snapshot_rv(self, round_id):
        rows = self.connection.execute(
            "SELECT DISTINCT resource_version FROM list_pages WHERE round_id=? AND status_code=200",
            (round_id,),
        ).fetchall()
        versions = {row["resource_version"] for row in rows}
        return next(iter(versions)) if len(versions) == 1 else None

    # ---- Watch 连接栅栏 ----

    def open_connection(self, round_id, cluster, scope, resource_version):
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO watch_connections(round_id,cluster,scope,resource_version,active) "
                "VALUES(?,?,?,?,1)",
                (round_id, cluster, scope, resource_version),
            )
        return cursor.lastrowid

    def fence_round_connections(self, round_id, reason):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE watch_connections SET active=0, ended_reason=? "
                "WHERE round_id=? AND active=1",
                (reason, round_id),
            )
            conn.execute(
                "UPDATE staged_events SET stale=1 WHERE round_id=? AND applied=0",
                (round_id,),
            )

    def close_connection(self, connection_id, reason):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE watch_connections SET active=0, ended_reason=? WHERE connection_id=?",
                (reason, connection_id),
            )

    def is_connection_active(self, connection_id):
        row = self.connection.execute(
            "SELECT active FROM watch_connections WHERE connection_id=?",
            (connection_id,),
        ).fetchone()
        return row is not None and bool(row["active"])

    # ---- 事件暂存 ----

    def stage_event(self, round_id, cluster, connection_id, event, capture_id):
        event_type = event["type"]
        obj = event.get("object", {}) or {}
        meta = obj.get("metadata", {}) or {}
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO staged_events(round_id,cluster,connection_id,event_type,uid,"
                "name,namespace,resource_version,capture_id,payload) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (round_id, cluster, connection_id, event_type,
                 meta.get("uid"), meta.get("name"), meta.get("namespace"),
                 str(meta.get("resourceVersion", "")), capture_id,
                 json.dumps(event, ensure_ascii=False)),
            )
        return cursor.lastrowid

    def mark_stale(self, staged_id):
        with self.transaction() as conn:
            conn.execute("UPDATE staged_events SET stale=1 WHERE id=?", (staged_id,))

    # ---- 目录晋升：整轮原子切换 ----

    def promote_round(self, round_id, cluster, snapshot_rv, present_keys, resume_token):
        """把一整轮 List + 暂存事件原子切换为当前视图。

        present_keys 为该轮覆盖 scope 内观察到的 ``(uid, scope)`` 集合；
        未在集合中出现、且 scope 非 restricted 的现存对象标记 deleted。
        restricted scope 内的对象不参与删除对账。
        """
        with self.transaction() as conn:
            restricted = {
                row["scope"] for row in conn.execute(
                    "SELECT scope FROM scope_coverage WHERE round_id=? AND state=?",
                    (round_id, COV_RESTRICTED),
                )
            }
            present_uids = {uid for uid, scope in present_keys if scope not in restricted}
            existing = conn.execute(
                "SELECT uid, namespace FROM objects WHERE cluster=? AND state='present'",
                (cluster,),
            ).fetchall()
            for row in existing:
                scope = row["namespace"]
                if scope in restricted:
                    continue
                if row["uid"] not in present_uids:
                    conn.execute(
                        "UPDATE objects SET state='deleted', last_event_round=?, "
                        "last_origin='list-reconcile', updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                        "WHERE cluster=? AND uid=?",
                        (round_id, cluster, row["uid"]),
                    )
                    conn.execute(
                        "INSERT INTO object_provenance(cluster,uid,origin,round_id,event_type,"
                        "resource_version) VALUES(?,?,?,?,?,?)",
                        (cluster, row["uid"], ORIGIN_LIST, round_id, "RECONCILED_ABSENT", snapshot_rv),
                    )
            conn.execute(
                "UPDATE rounds SET status='promoted', promoted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), "
                "snapshot_rv=? WHERE round_id=?",
                (snapshot_rv, round_id),
            )
            prior = conn.execute(
                "SELECT round_id FROM rounds WHERE cluster=? AND status='promoted' AND round_id<>?",
                (cluster, round_id),
            ).fetchall()
            for prior_row in prior:
                conn.execute(
                    "UPDATE watch_connections SET active=0, ended_reason='superseded' "
                    "WHERE round_id=? AND active=1",
                    (prior_row["round_id"],),
                )
                conn.execute(
                    "UPDATE staged_events SET stale=1 WHERE round_id=? AND applied=0",
                    (prior_row["round_id"],),
                )
            conn.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (f"active_round:{cluster}", str(round_id)),
            )
            conn.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (f"resume_token:{cluster}", str(resume_token)),
            )

    # ---- 单条观察写入（List 页对象 / Watch 事件） ----

    def upsert_list_object(self, round_id, cluster, scope, obj, page_id):
        meta = obj["metadata"]
        uid, name, namespace = meta["uid"], meta["name"], meta["namespace"]
        rv = str(meta["resourceVersion"])
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT 1 FROM objects WHERE cluster=? AND uid=?", (cluster, uid)
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO objects(cluster,uid,name,namespace,resource_version,state,"
                    "first_seen_round,last_event_round,last_origin,payload) "
                    "VALUES(?,?,?,?,?,'present',?,?,?,?)",
                    (cluster, uid, name, namespace, rv, round_id, round_id,
                     ORIGIN_LIST, json.dumps(obj, ensure_ascii=False)),
                )
            else:
                conn.execute(
                    "UPDATE objects SET name=?,namespace=?,resource_version=?,state='present',"
                    "last_event_round=?,last_origin=?,"
                    "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),payload=? "
                    "WHERE cluster=? AND uid=?",
                    (name, namespace, rv, round_id, ORIGIN_LIST,
                     json.dumps(obj, ensure_ascii=False), cluster, uid),
                )
            conn.execute(
                "INSERT INTO object_provenance(cluster,uid,origin,round_id,page_id,capture_id,"
                "event_type,resource_version) VALUES(?,?,?,?,?,?,'LISTED',?)",
                (cluster, uid, ORIGIN_LIST, round_id, page_id,
                 self._page_capture(conn, page_id), rv),
            )

    def _page_capture(self, conn, page_id):
        if page_id is None:
            return None
        row = conn.execute("SELECT capture_id FROM list_pages WHERE id=?", (page_id,)).fetchone()
        return row["capture_id"] if row else None

    def already_seen(self, cluster, uid, resource_version):
        """按 (uid, 不透明 rv) 相等去重；不允许任何版本大小比较。"""
        row = self.connection.execute(
            "SELECT 1 FROM object_provenance WHERE cluster=? AND uid=? AND resource_version=? LIMIT 1",
            (cluster, uid, resource_version),
        ).fetchone()
        return row is not None

    def apply_watch_event(self, round_id, cluster, connection_id, staged_id, event,
                          capture_id, advance_resume=True):
        event_type = event["type"]
        obj = event["object"]
        meta = obj["metadata"]
        uid = meta.get("uid")
        name = meta.get("name")
        namespace = meta.get("namespace")
        rv = str(meta.get("resourceVersion", ""))
        with self.transaction() as conn:
            if event_type == EVT_BOOKMARK:
                conn.execute(
                    "INSERT INTO object_provenance(cluster,uid,origin,round_id,"
                    "staged_event_id,connection_id,capture_id,event_type,resource_version) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (cluster, uid or "", ORIGIN_WATCH, round_id, staged_id,
                     connection_id, capture_id, EVT_BOOKMARK, rv),
                )
                conn.execute("UPDATE staged_events SET applied=1 WHERE id=?", (staged_id,))
                if advance_resume:
                    conn.execute(
                        "INSERT INTO kv(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (f"resume_token:{cluster}", rv),
                    )
                return "bookmark"
            if uid is None:
                conn.execute("UPDATE staged_events SET stale=1 WHERE id=?", (staged_id,))
                return "ignored"
            if self._seen_in_conn(conn, cluster, uid, rv):
                conn.execute("UPDATE staged_events SET applied=1 WHERE id=?", (staged_id,))
                return "duplicate"
            existing = conn.execute(
                "SELECT * FROM objects WHERE cluster=? AND uid=?", (cluster, uid)
            ).fetchone()
            if event_type == EVT_DELETED:
                if existing is None:
                    action = "unmatched-delete"
                else:
                    conn.execute(
                        "UPDATE objects SET state='deleted',name=?,namespace=?,"
                        "resource_version=?,last_event_round=?,last_origin='watch',"
                        "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                        "WHERE cluster=? AND uid=?",
                        (name, namespace, rv, round_id, cluster, uid),
                    )
                    action = "deleted"
            elif event_type in (EVT_ADDED, EVT_MODIFIED):
                if existing is None:
                    conn.execute(
                        "INSERT INTO objects(cluster,uid,name,namespace,resource_version,state,"
                        "first_seen_round,last_event_round,last_origin,payload) "
                        "VALUES(?,?,?,?,?,'present',?,?,?,?)",
                        (cluster, uid, name, namespace, rv, round_id, round_id,
                         ORIGIN_WATCH, json.dumps(obj, ensure_ascii=False)),
                    )
                else:
                    conn.execute(
                        "UPDATE objects SET name=?,namespace=?,resource_version=?,state='present',"
                        "last_event_round=?,last_origin='watch',"
                        "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),payload=? "
                        "WHERE cluster=? AND uid=?",
                        (name, namespace, rv, round_id,
                         json.dumps(obj, ensure_ascii=False), cluster, uid),
                    )
                action = "upserted"
            else:
                conn.execute("UPDATE staged_events SET stale=1 WHERE id=?", (staged_id,))
                return "ignored"
            conn.execute(
                "INSERT INTO object_provenance(cluster,uid,origin,round_id,staged_event_id,"
                "connection_id,capture_id,event_type,resource_version) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (cluster, uid, ORIGIN_WATCH, round_id, staged_id,
                 connection_id, capture_id, event_type, rv),
            )
            conn.execute("UPDATE staged_events SET applied=1 WHERE id=?", (staged_id,))
            if advance_resume:
                conn.execute(
                    "INSERT INTO kv(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"resume_token:{cluster}", rv),
                )
            return action

    @staticmethod
    def _seen_in_conn(conn, cluster, uid, rv):
        row = conn.execute(
            "SELECT 1 FROM object_provenance WHERE cluster=? AND uid=? AND resource_version=? LIMIT 1",
            (cluster, uid, rv),
        ).fetchone()
        return row is not None

    # ---- scope 覆盖证据 ----

    def record_coverage(self, cluster, scope, round_id, state, capture_id):
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO scope_coverage(cluster,scope,round_id,state,evidence_capture_id) "
                "VALUES(?,?,?,?,?) ON CONFLICT(cluster,scope,round_id) "
                "DO UPDATE SET state=excluded.state, evidence_capture_id=excluded.evidence_capture_id",
                (cluster, scope, round_id, state, capture_id),
            )

    # ---- 查询视图（归档人员界面） ----

    def current_objects(self, cluster, include_deleted=False):
        sql = "SELECT * FROM objects WHERE cluster=?"
        if not include_deleted:
            sql += " AND state='present'"
        sql += " ORDER BY namespace, name, uid"
        return [dict(row) for row in self.connection.execute(sql, (cluster,)).fetchall()]

    def object_by_name(self, cluster, namespace, name):
        """name 只是当前定位：返回该定位上的全部 UID 生命期。"""
        rows = self.connection.execute(
            "SELECT * FROM objects WHERE cluster=? AND namespace=? AND name=? ORDER BY first_seen_round",
            (cluster, namespace, name),
        ).fetchall()
        return [dict(row) for row in rows]

    def provenance(self, cluster, uid):
        rows = self.connection.execute(
            "SELECT * FROM object_provenance WHERE cluster=? AND uid=? ORDER BY id",
            (cluster, uid),
        ).fetchall()
        return [dict(row) for row in rows]

    def coverage(self, cluster):
        active = self.active_round(cluster)
        if active is None:
            return []
        rows = self.connection.execute(
            "SELECT * FROM scope_coverage WHERE cluster=? AND round_id=? ORDER BY scope",
            (cluster, active),
        ).fetchall()
        return [dict(row) for row in rows]

    def stale_events(self, cluster):
        rows = self.connection.execute(
            "SELECT * FROM staged_events WHERE cluster=? AND stale=1 ORDER BY id", (cluster,)
        ).fetchall()
        return [dict(row) for row in rows]

    def rounds(self, cluster):
        rows = self.connection.execute(
            "SELECT * FROM rounds WHERE cluster=? ORDER BY round_id", (cluster,)
        ).fetchall()
        return [dict(row) for row in rows]

    def round_present_uids(self, round_id, cluster):
        rows = self.connection.execute(
            "SELECT DISTINCT uid FROM object_provenance WHERE round_id=? AND cluster=? "
            "AND origin='list' AND event_type='LISTED'",
            (round_id, cluster),
        ).fetchall()
        return [row["uid"] for row in rows]
