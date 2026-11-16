# Kubernetes 目录接入设计

只做只读采集：整条链路只发 `GET`，没有任何创建、更新或删除调用。设计目标是让
当前视图（catalog）在分页、断线、重连、进程重启和旧连接回流时都不混入没有证据
的对象，并且每个对象都能追溯到具体的一次 List 分页或一条 Watch 事件。

## 1. 身份与生命期

- 对象主键是 `(cluster, uid)`。`name`、`namespace` 只是当前定位字段，可随对象
  变化；同名对象删除后以新 UID 重建是两条独立生命期。
- `objects` 表保存当前视图，`object_history` 表追加每次变化。旧 UID 的
  `DELETED` 事件只按 UID 命中旧生命期，没有能力把新 UID 的同名对象置为删除。
- 收到无法识别 UID 的删除事件时记为 `uid_unknown` 围栏证据，不创建墓碑对象。

## 2. 轮次（round）与分页暂存

一次全量重列举是一个 round，状态机为 `running → promoted` 或
`running → abandoned`。

1. round 开始后，每个范围（`cluster` 或 `namespace:名称`）逐页 List，对象先写
   `staged_objects`，每页记录 `list_pages`（入参 continue、出参 continue、
   快照 RV、HTTP 状态、原始归档 capture_id）。
2. 同一范围所有分页必须共享同一个快照 resourceVersion；页间 RV 不一致即整轮
   失败，防止把不同时点的页面拼接成伪全量。
3. 任一页非 200（含模拟的 500/400）或中途异常，round 标记 `abandoned`，暂存
   数据保留作证据但永不提升；上一个 `promoted` 轮次的视图继续有效。
4. 所有范围都拿到完整证据后，`promote_round` 在单个 SQLite 事务里完成：
   暂存对象 upsert 进当前视图、覆盖范围内快照缺失的存活 UID 标记删除、
   写入 `scope_status`、旧轮次连接全部置 `superseded`。事务失败则视图不变。

因此"未完成的一组分页"在任何时刻都不会被宣布为全量：活动轮次永远只指向
`promoted` 的那一个。

## 3. List → Watch 的交接位置

- 交接位置只取每个范围**首页** List 的 `metadata.resourceVersion`（快照 RV），
  写入 `scope_results.list_rv` 与 `scope_status.progress_rv`。
- 分页期间发生的变更不在本轮 List 快照里；Watch 用该快照 RV 打开，服务端回放
  其后的事件，采集器把它们作为同一 round 的 watch 证据补齐。
- `continue` 令牌对采集器不透明，只原样回传，不解析其内部结构。

## 4. Watch、断线与 410

- 每条 Watch 连接在 `connections` 登记，绑定 `round_id` 与 scope，起始 RV 必须
  来自 List 证据；没有证据 RV 时拒绝开流，而不是构造一个。
- 普通传输断线：连接关闭，下一拍用**已持久化的 progress RV 原样回传**重连，
  不修改 RV。
- 收到 410 `Expired`（初始响应或流内 ERROR 状态）：RV 已失效，禁止递增、加一、
  字典序猜测或复用旧值。采集器置 `relist_pending`，重新执行一轮完整 List，
  用新快照 RV 建立新连接，并通过 promote 原子切换目录。
- BOOKMARK 只更新 `progress_rv`，不 upsert 任何业务对象，不写对象历史。
- 同一对象 RV 相等的重复事件按 `duplicate_rv` 去重；RV 只做相等判断。

## 5. 不透明 resourceVersion 契约

采集器代码中不存在 RV 的数值解析或大小比较，只有三种操作：保存、相等判断、
原样放进查询串。模拟器默认生成随机令牌，专门用于在测试中破坏任何隐含的排序
假设；模拟器内部的历史下标仅用于回放区间判定，从不对外暴露。

## 6. 旧连接围栏（fencing）

round 切换后，旧连接不会被静默丢弃：

- 旧连接在 `connections` 中标记 `superseded`，读取器移入
  `superseded_streams` 继续排空队列；
- 旧连接的业务事件写入 `watch_events`，`applied=0` 且
  `fence_reason='stale_round_connection'`，只入证、不应用；
- 旧连接的 BOOKMARK 直接丢弃（进度只属于它自己的旧轮次）；
- 连接状态非 `open`、轮次不匹配或连接 ID 无法识别时，事件走同样的围栏路径。

这样旧 watch 里迟到的删除事件即使回流，也无法触碰新轮次的当前视图。

## 7. 403 与采集范围受限

- List 或 Watch 返回 403 时，scope 标记 `restricted`，保留状态码与消息证据，
  不建立 Watch、不产生分页证据。
- 受限范围**不参与** promote 的缺失对账：其中的既有对象原样保留，不会被当成
  "快照里没有 → 已删除"。
- 模拟器在 cluster 级 List/Watch 中同样过滤受限命名空间，防止受限对象从全量
  范围侧漏入。
- `coverage` 查询逐范围返回 `watching / restricted / never_covered`，
  从未拿到任何证据的范围显式展示为 `never_covered`，而不是默默视为空。

## 8. 重启恢复

所有状态都在 SQLite。进程重启后 `resume()` 读取活动 round 与各 scope 的
progress RV，从原 RV 重新打开 Watch；没有活动 round 才执行首次全量列举。
abandoned round 永远不会被恢复成活动轮次。

## 9. 证据模型与查询

| 查询 | 入口 | 回答的问题 |
| --- | --- | --- |
| 对象溯源 | `object_provenance(cluster, uid)` | 当前对象来自哪次 List 分页（`origin_page_id`）或哪条 Watch 事件（`origin_event_id`），完整变化历史 |
| 轮次证据 | `round_evidence(round_id)` | 该轮每范围结果、每页分页证据、每条 Watch 事件及围栏原因 |
| 覆盖范围 | `coverage(cluster, scopes)` | 哪些范围在看、哪些受限、哪些从未覆盖 |
| 围栏统计 | `fence_stats(cluster)` | 各类被拒绝事件的数量 |

List 页与 Watch 事件同时写入原始 HTTP 归档（`Archive`），目录证据通过
`capture_id` 与原始响应正文关联，归档人员可以回到未经解释的原始字节。

## 10. 本地模拟器

`kube_sim.py` 在单进程内提供 Kubernetes 只读 API 子集：

- `GET /api/v1/pods`、`GET /api/v1/watch/pods`、
  `GET /api/v1/namespaces/{ns}/pods[/watch]`；
- 分页 List（首页生成快照专属 RV 与 continue 令牌，页间共享快照）；
- 行分隔 JSON 的流式 Watch、BOOKMARK 心跳、410 Expired、403 Forbidden；
- `POST /__admin__/mutate|inject|expire|drop|forbid|flags` 仅用于测试编排，
  采集器从不调用这些端点（测试单独断言采集流量全部为 GET 且不含 `/__admin__`）。

模拟器不接受任何业务写操作，不需要 kubeconfig，不连接外部集群。
