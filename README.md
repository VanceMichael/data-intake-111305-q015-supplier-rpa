# 供应商托管集群的 Kubernetes 资源目录采集

这是资料接收程序的基础版。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。当前不生成面向业务的有效版本，也不自动执行门户提交；重复观察会分别保留，原始内容按摘要复用。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q supplier_rpa tests
python3 -m supplier_rpa demo
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，并输出原样记录；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。

## 常驻来源与接收

```sh
python3 -m supplier_rpa serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m supplier_rpa capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。业务解释可与原始接收独立演进。

## Kubernetes 目录接入（只读 List/Watch）

`kube_sim.py` 提供本地 Kubernetes API 模拟器（分页 List、流式 Watch、BOOKMARK、410/403 与测试管控端点），`kube_client.py` 是只发 GET 的流式客户端，`catalog.py` 是目录状态机。设计边界见 `docs/catalog-design.md`，要点：

- 对象身份是 `(cluster, uid)`，名称只是当前定位；同名重建产生独立生命期，旧 UID 的迟到删除抹不掉新对象。
- 一次全量列举是一个轮次：分页对象先进暂存区，所有覆盖范围分页完整后才在单事务内切换目录；任何一页失败则整轮废弃，旧视图继续有效，半成品不会被宣布为全量。
- List 与 Watch 的交接位置只取首页快照 resourceVersion；RV 全程按不透明标识保存、相等判断、原样回传，禁止数值或字典序比较。
- Watch 断线用已保存的进度 RV 续接；收到 410 不猜测版本号，而是重新列举出有证据的新目录再原子切换。
- 旧轮次连接被围栏：迟到事件只入证（`fence_reason='stale_round_connection'`）不应用；BOOKMARK 只更新进度，不产生业务对象。
- 403 范围标记 `restricted` 并保留证据，其中旧对象不参与删除对账；`coverage` 显式展示 `restricted` 与 `never_covered` 范围。
- 每个对象可溯源到具体 List 分页或 Watch 事件，目录证据通过 `capture_id` 关联原始 HTTP 归档。

```sh
python3 -m supplier_rpa kube-demo
python3 -m supplier_rpa kube-serve            # 另开终端
python3 -m supplier_rpa kube-sync --base-url http://127.0.0.1:端口 --cluster managed-a \
    --catalog .runtime/catalog.sqlite3 --archive .runtime/raw.sqlite3 \
    --scope cluster --scope namespace:restricted
python3 -m supplier_rpa kube-query object   --catalog .runtime/catalog.sqlite3 --cluster managed-a --uid <UID>
python3 -m supplier_rpa kube-query round    --catalog .runtime/catalog.sqlite3 --cluster managed-a --round-id <轮次>
python3 -m supplier_rpa kube-query coverage --catalog .runtime/catalog.sqlite3 --cluster managed-a --scope cluster
```

`kube-demo` 在临时目录内完成分页列举、同名重建、410 重列举、旧连接围栏与 403 受限的完整演练，结束自动清理。契约测试见 `tests/test_catalog_contract.py`，对应 `fixtures/business_cases.json` 的三个业务样例。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；基础版测试验证的是原始采集和协议，不代表业务处理已经具备这些能力。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `supplier_rpa` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m supplier_rpa demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
