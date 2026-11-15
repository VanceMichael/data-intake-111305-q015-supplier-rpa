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

## Kubernetes List / Watch 目录接入

在原始 HTTP 接收之上，`kubesim.py` 提供本地 Kubernetes API 子集模拟器（分页 List、NDJSON Watch、410 压缩、403 受限，无需外部集群），`kubeclient.py` 执行只读 List/Watch，`catalog.py` 持久化目录状态与证据链，`ingestor.py` 编排轮次、分页交接、Watch 续接与栅栏切换。核心边界：

- 身份按 `(cluster, uid)` 保留对象生命期，name 只是当前定位；删除按 UID 命中，同名重建与旧连接迟到删除互不影响。
- 每轮重列举独立成 round，未完成的分页不会被宣布为全量；Watch 事件按轮次栅栏化，旧连接回流只入审计不进当前视图。
- resourceVersion 全程按不透明标识存取与回传，禁止数值/字典序比较；410 必须全新 List 重建目录，不猜版本号；BOOKMARK 只表示进度。
- 403 记为采集范围受限，保留该范围旧对象，不伪造删除；覆盖表与溯源表可回答“哪些范围还没有覆盖证据”和“每个对象来自哪次 List/Watch”。

```sh
python3 -m supplier_rpa kube-demo
```

演示在临时库上完成首轮分页 List、同名对象删除重建、压缩后 410 重列举、受限 namespace 403 四个环节，并输出当前视图、按名称的 UID 生命期、覆盖证据、轮次与溯源。加 `--archive`/`--catalog` 可把两个 SQLite 库落到指定路径复查。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；基础版测试验证的是原始采集和协议，不代表业务处理已经具备这些能力。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `supplier_rpa` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m supplier_rpa demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
