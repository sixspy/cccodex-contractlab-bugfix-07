# 入站契约

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 入站契约 |
| 目录 | `05-contractlab` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10732 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

字段增删改类型、宽严模式、影子契约、缓冲窗、生产者消费者双视角

## 避开

别做成质检签发换皮；别做成纯表单 CRUD

## 互斥切口候选

- 破坏性变更直接拒收 vs 双写缓冲
- 影子只读试跑 vs 双轨分开展示
- 契约回滚整版原子切换 vs 按生产者摘流

## 拒收副作用（收暑双路径）

拒收不得推进通过计数。收暑当天选定后写入 `拒收副作用簿`（表 `reject_side_effect_book`），当天禁止改口；
偏离即拒绝并记「副作用偏了」（表 `side_effect_events`）。

| 收暑 | 拒收怎么写 | 对账怎么核 |
|------|------------|------------|
| 乙（默认） | 拒收批次写入 `ingress_batches.reject_snapshot` 拒收前值列 | 排除拒收批次现场重算，与落库账册逐日比对 |
| 甲 | 只追加拒收批次与原因，前值留在副作用簿事件 | 快照保持拒收前值 |

- 计数落库可核：`reconcile_ledger` 按天持久化通过/拒收/缓冲/入站，`/api/reconcile` 返回 `ledger_ok`、`snapshot_passed`、`passed_excl_reject`。
- 拒收与通过同抢写排队：`/api/ingress/validate` 共用一道门，后到只记「路径争用」，不另起通过计数。
- 同源：`/api/ingress`、`/api/rejects`、`/api/review-queue`、`/api/reject-policy`、`/api/reconcile` 均读同一批拒收批次。
- 不改库文件：`init_db()` 对现行 `data/app.db` 原地加列建表。

## 启动（一键）

`powershell
cd projects/05-contractlab
docker compose up -d --build
`

浏览器：http://127.0.0.1:10732/  
健康检查：GET http://127.0.0.1:10732/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10602
python -m pytest -q
`
