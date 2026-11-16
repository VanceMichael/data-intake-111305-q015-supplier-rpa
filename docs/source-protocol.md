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

完整的本地模拟器与采集状态机由 `supplier_rpa/kube_sim.py`、`kube_client.py`、`catalog.py` 实现，边界设计见 `docs/catalog-design.md`：

- List：`GET /api/v1/pods` 与 `GET /api/v1/namespaces/{ns}/pods`，支持 `limit` 与 `continue` 分页；首页返回快照专属 resourceVersion，同轮各页共享同一快照，页间 RV 不一致或分页失败时整轮废弃。
- Watch：`GET /api/v1/watch/pods` 与命名空间变体，行分隔 JSON 流，支持 BOOKMARK 心跳、流内 ERROR（410 Expired）与连接断开；未知 resourceVersion 直接 410。
- 403：`namespace/restricted` 的 List 与 Watch 均拒绝，采集器据此把范围标记为 restricted，不伪造删除。
- 管控端点 `POST /__admin__/{mutate,inject,expire,drop,forbid,flags}` 仅供测试编排（注入迟到事件、触发 410/断线、切换授权），采集器从不调用；采集流量经测试断言全部为 GET。
- 原始 HTTP 响应仍由 `Archive` 逐条归档，目录中的分页与事件证据通过 `capture_id` 回链到原始正文。
