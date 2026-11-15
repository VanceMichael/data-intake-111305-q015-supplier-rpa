# 来源协议

## 记录与身份

默认来源入口为 `/feed`，媒体格式为 `json`。来源身份字段是 `cluster`、`capture`。这些字段表示来源自身声明，不等同于已经解决的业务实体身份。输入顺序是一次接收的观察顺序，并不保证业务修订有序。

`/observations/1` 等地址允许逐条读取合成观察，返回 `items` 数组。正文、页内嵌入数据与CSV均保留供方原文，不在协议层推断当前有效值。网页中的 `source-records` 是门户向下载客户端提供的公开数据块；表单还带有隐藏控件，令牌用于本地协议状态检查。

## 传输响应

`/feed` 有 ETag `feed-1`，可发送 `If-None-Match` 得到304。`/rate-limited` 返回429和两秒的Retry-After；`/unstable` 首次503，其后200；`/truncated` 声明长度大于实际传送长度并断开。`/range` 支持单段字节范围及416，`/range-ignored` 故意返回整份200，不能假定所有供方都会履行范围请求。

每次独立启动来源都有独立计数器。一个路由在 `origin.json` 中若是响应数组，依请求次序前进，最后一个响应保持不变。可通过编辑一份场景副本再用 `serve --scenario 文件` 启动不同交错序列。

## 表单回执

演练门户只接受本地POST `/portal/submit`，表单字段为 `id`、`csrf`，当前令牌由场景的 `portal.csrf` 声明。过时令牌409，缺少身份422。添加 `lose_response=yes` 会先受理再断开连接；随后GET `/portal/status?id=标识` 可看到真实回执。门户不提供提交幂等键，接收程序默认不执行提交。该端点用于受理确认和供应商核对的本地演练，不连接任何真实门户。

## 领域样例

- `uid_recreation`：同名新UID之后旧删除回流；业务侧关注不删新生命周期。
- `watch_expiry`：Watch位置过期410；业务侧关注重列举切换，不猜测版本顺序。
- `namespace_forbidden`：只读范围缩小到403；业务侧关注标受限，不伪造目录删除。

另外提供 `/api/v1/pods`、`/api/v1/watch`、`/api/v1/namespaces/restricted/pods`。响应内容和版本由场景文件逐项声明，便于核对实际收到的来源资料。

## 领域接入契约

采用Kubernetes只读List、Watch、Status响应子集。resourceVersion按本项目不透明字符串契约处理，禁止按数值或字典序推导先后。模拟器通过固定路由及响应序列提供410和403，不需要真实kubeconfig，不读取Secrets，也不提交任何集群变更。

### 身份与轮次

- 对象稳定身份是 `(cluster, uid)`。name 与 namespace 只是当前定位，可被后续观察更新；删除严格按 UID 命中，同名对象删除后重建会得到两条独立生命期，旧 Watch 的迟到删除不会抹掉新 UID。
- 每次（重）列举开启一个新的 round，Watch 连接绑定 round。晋升新轮次时旧连接全部栅栏化，旧连接回流的事件只进入 `staged_events`（`stale=1`）作为审计证据，不写入当前对象视图，因此重启与旧连接回流都不会把不同轮次的对象混入当前视图。

### List 分页与事件交接

- 一轮 List 由各 scope 的分页组成，跟随服务端 `continue` 令牌，不自行推断偏移；同一快照所有页的 resourceVersion 必须相等，否则整轮判失败。
- 任何一页失败、缺失 resourceVersion 或快照版本漂移，整轮标记 `failed`：旧目录保持为当前视图，未完成的分页不会被宣布为全量。
- 重列举可先把 Watch 挂在旧检查点上，分页期间到达的事件进入暂存表；晋升前按到达顺序回放，按 `(uid, resourceVersion)` 相等去重，不做版本大小比较。重列举后权威续接点是新 List 快照的 resourceVersion。

### Watch 续接与 410

- 干净断连时凭最后一个 BOOKMARK/事件携带的不透明 resourceVersion 原样续接。
- 收到 410 Gone 时不猜任何版本号、不重试旧 token，直接开始一轮全新 List 重建有依据的目录。
- BOOKMARK 只表示进度（更新 resume token 并留证据），不是业务对象，不会产生或覆盖任何目录对象。

### 采集范围与覆盖证据

- 某 namespace 返回 403 时记为 `restricted` 覆盖证据，该 scope 内旧对象保留为 present，不参与重列举对账删除。
- `scope_coverage` 记录每个 scope 在当前轮次是 covered 还是 restricted；没有任何记录的 scope 即“尚无覆盖证据”，归档人员可直接识别未覆盖范围。
- `object_provenance` 记录每个对象每一次观察来自哪次 List 页（page_id）或哪条 Watch 事件（staged_event_id/connection_id），并回链到原始归档 capture，可逐条核对来源报文。
