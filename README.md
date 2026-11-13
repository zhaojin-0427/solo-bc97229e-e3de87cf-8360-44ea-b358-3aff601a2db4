# 差分隐私公交客流查询服务

FastAPI + SQLite 实现的差分隐私客流查询 API：乘车 CSV 一次导入后只读，查询返回
**去重乘客数 + 独立 Laplace(0, 1/ε) 噪声**（不返回真值），并在两个 API 进程共享
同一 SQLite 库的前提下，完成预算记账、相同查询去重与崩溃租约接续。

## 一、需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 一次导入 CSV（匿名乘客号、上车站），之后只读 | `POST /import` 单次导入，`meta.imported` 锁定，重复导入返回 409 |
| 非空站点集合 + ε∈{0.1, 0.2, 0.5} | Pydantic 校验，空集合/非法 ε 返回 422 |
| 去重乘客数 + 独立 Laplace(0,1/ε)，不返回真值 | `app/privacy.py` 逆变换采样（SystemRandom）；真值只在计算瞬间存在，库里只存加噪值 |
| 总预算 1，新查询预留 ε，成功后扣 ε | 预算账本 `budget_ledger`：reserve(+ε) → 成功时 release(-ε)+commit(-ε)，同一事务 |
| 同集合、同 ε 永久复用原结果，顺序无关，不重复扣费 | 站点去空白/去重/排序后 SHA-256 得 `canonical_key`，`(key, ε)` 唯一缓存；命中直接返回，零扣费、同噪声 |
| 预留+已用不得超过 1，不足拒绝 | 预算检查与预留同在 `BEGIN IMMEDIATE` 事务，不足返回 HTTP 402，不留任何预留 |
| 两个 API 进程共享库，相同查询复用在途任务，不重复计算 | 部分唯一索引保证每 `(key, ε)` 至多一个 active 任务；后来者成为跟随者只读轮询 |
| 统计不占库写事务，其他请求保持响应 | WAL + 短连接；统计走独立只读连接，写事务仅覆盖记账/状态变更 |
| 结果与预留转实扣共同持久化后才返回；响应丢失不重新扰动 | 结果插入、任务置成功、release+commit 在一个事务提交；重试命中缓存拿同一份噪声 |
| 失败无结果并退还预留 | 失败事务：任务置 failed/active=0 + release(-ε)；不写结果，重试才重新采样 |
| 执行进程退出后自动接续；存活执行不被接管 | 租约 `lease_expires_at` + 心跳线程续租；过期后其他进程条件 UPDATE 接管，generation+1 |
| 任务状态及预算查询 | `GET /tasks`、`GET /tasks/{id}`、`GET /budget`；交互页 `GET /` |

## 二、目录结构

```
app/
  config.py            环境变量配置（库路径、租约、心跳、演示延迟等）
  db.py                SQLite 连接、WAL、schema、写事务助手
  privacy.py           Laplace 噪声
  budget.py            预算聚合（used/reserved/remaining）
  models.py            请求/响应模型与站点集合规范化
  importer.py          CSV 解析与一次性导入
  tasks.py             选举、租约、心跳、执行/接续、预算事务（核心）
  api.py               FastAPI 路由
  static/index.html    交互式演示页面（导入/查询/预算/任务）
tests/                 22 个测试（单元 + 单进程 + 双真实进程集成，含 SIGKILL 崩溃接续）
Dockerfile  docker-compose.yml  nginx.conf  sample_rides.csv
```

## 三、启动（Docker Compose）

```bash
docker compose up --build
# 交互入口（nginx 轮询两个 API 进程，响应头 X-Worker-Id 标识实际进程）
#   演示页面:  http://localhost:8080/
#   Swagger:   http://localhost:8080/docs
```

拓扑：`api-a`、`api-b` 两个独立 uvicorn 进程，挂载同一个命名卷上的 SQLite 文件；
`nginx` 默认轮询负载均衡。数据库文件落在 `dp_data` 卷的 `/data/dp_transit.db`。

重置环境（清空数据与预算）：

```bash
docker compose down -v
```

## 四、快速演示（curl）

```bash
# 1. 导入（仅一次）
curl -X POST localhost:8080/import -F "file=@sample_rides.csv;type=text/csv"

# 2. 首次查询：预留并实扣 ε=0.2
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"stations":["人民广场","陆家嘴"],"epsilon":0.2}'

# 3. 逆序同集合同 ε：reused=true、charged_epsilon=0、噪声完全相同
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"stations":["陆家嘴","人民广场"],"epsilon":0.2}'

# 4. 预算与任务
curl localhost:8080/budget
curl localhost:8080/tasks
```

## 五、本地无 Docker 运行与测试

```bash
pip install -r requirements.txt
# 两个进程共享同一个 DB 文件
DP_DB_PATH=/tmp/dp.db uvicorn app.api:app --port 9101 &
DP_DB_PATH=/tmp/dp.db uvicorn app.api:app --port 9102 &

pytest -q          # 22 个测试，含双进程并发去重与 SIGKILL 租约接续
```

测试/演示用环境变量：

- `DP_SIMULATED_EXEC_DELAY`：执行阶段人为延迟秒数，制造“已预留未提交”窗口，
  便于观察在途任务与崩溃接续；生产置 0（默认）。
- `DP_LEASE_SECONDS`（默认 15）、`DP_HEARTBEAT_SECONDS`（默认 3）。

## 六、关键设计说明

1. **协调只靠数据库**：无内存锁、无外部队列。跨进程互斥由
   `BEGIN IMMEDIATE` 写事务 + `tasks` 上的部分唯一索引
   （`WHERE active=1`）强制，并发 INSERT 竞态由 SQLite 约束拒绝并重试。
2. **预算账本不可变**：只追加带符号条目，`reserved = Σreserve + Σrelease`、
   `used = |Σcommit|`，任何状态都可重放审计；拒绝预留时不写任何条目。
3. **崩溃接续不重复扣费**：接管是对**同一任务行**的条件 UPDATE，沿用其预留；
   成功提交时只做一次 release+commit。
4. **真值隔离**：`rides` 表只服务于瞬时 `COUNT(DISTINCT)` 统计，
   `query_results` 仅保存加噪结果，任何接口都不返回真值。
