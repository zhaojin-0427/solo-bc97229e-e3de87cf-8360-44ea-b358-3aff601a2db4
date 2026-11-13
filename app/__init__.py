"""差分隐私客流查询服务。

模块划分：
- config:   环境变量与可调参数（租约时长等）
- db:       SQLite 连接、WAL 初始化、写事务助手
- budget:   预算账本聚合（已用 / 预留 / 剩余）
- privacy:  Laplace 噪声
- tasks:    任务去重、租约选举、执行与接续
- api:      FastAPI 路由
"""
