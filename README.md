# 岩川 · 设备遥测索引试验室

为岩川设备可靠性组 12 人把检索负载、索引候选与真实 MySQL 试验串成可复查工作流。

## 业务场景
上游来自泵站温升告警与值班 SQL 文件。过去同事在表格记录列顺序、手动建索引后截取计划，存在参数版本混乱、前后结果不一致未发现、写入代价未记录三项痛点。本站生成固定 SELECT，在专用 5,000 行遥测库内串行建索引、采集执行计划、保存输入快照，交给可靠性复核人判断。

模块：工作负载录入/编辑、候选列序设计、后台试验队列、真实结果对比/JSON 导出；管理员另有审计。核心实体 workloads / candidates / experiments，样本表 telemetry。

## 统计重采样对照
新增「重采样对照」试验类型，专门回答一种现场事故：读者仍走索引，但遥测读数集中到少数设备后，负载却开始扫全表。

- 在按试验 ID 命名的独立副本表 `telemetry_cmp_<id>` 上执行，不触碰 `telemetry` 与索引试跑；试验成功后删除副本，失败/中断时保留供核验。
- 复制固定 5,000 行样本 → 建立候选索引并显式 `ANALYZE` 建立统计基线 → 写入一批确定性偏斜遥测。
- 「重新采样前」先执行 `FLUSH TABLE` 重载磁盘上的上一次采样统计：新增遥测此时已经可查，但优化器仍按旧统计估算。
- 显式 `ANALYZE` 重采样后，用同一查询、同一索引、同一批样本再测一遍；两臂都采集 EXPLAIN、估计扫描行、Handler 实查读取行、返回行与中位耗时。
- 副本表 `STATS_AUTO_RECALC=0`，并要求全局 `innodb_stats_on_metadata=0`；两臂前后各读一次统计指纹并比对，确认后台自动采样没有混进对照。
- 结论门禁：两侧查询结果 SHA-256 对账一致、索引结构一致、重采样前后统计确实变化、重采样前已能查到新增遥测，才给出「计划改变 / 计划未变」的结论，否则标记为不可比较并列出原因。
- 中断续跑：worker 崩溃或进程重启后，任务停在「执行中」。页面可点「续跑」，worker 会先核验留存副本、样本指纹与「重新采样前」结果；核验通过则续跑，核验不过（例如重新采样已生效但成绩尚未保存）就丢弃前一份证据、整对重测，并在证据里记录被作废的尝试。
- 不可变快照记录候选列、负载条件、偏斜行数与聚焦传感器；样本由固定公式生成，同样输入必得同样一批行。

该对照需要数据库账号拥有 `FLUSH_TABLES` 权限。`docker-compose.yml` 通过 `backend/scripts/mysql-init.sql` 在数据卷首次初始化时授予；已有数据卷若不重建，启动日志会提示缺少该权限，对照试验会以明确错误失败而不是给出错误结论。

## 快速启动
```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
前端 http://localhost:18241 ，后端 OpenAPI http://localhost:19241/docs ，MySQL localhost:17741（trial / trialpass / indextrial）。结束 `docker compose down -v --remove-orphans` 会清除本地数据。

| 角色 | 账号 | 权限 | 典型动作 |
|---|---|---|---|
| admin | admin@index.local | 全部、审计 | 复查谁执行了哪次试验 |
| operator | operator@index.local | 新建/编辑/执行/导出 | 保存候选并执行 |
| viewer | viewer@index.local | 读取/导出 | 复核证据，写请求返回403 |

统一密码 `Trial2026!`，仅为本地演示。JWT 8 小时，密码 PBKDF2。公开部署前须替换凭据、JWT 密钥及网络访问策略。

## 一天剧本
1. 09:00 operator 登录，查看「泵站17温升排查」。
2. 09:05 查看 sensor_id=17、2026-11-01 至 2026-11-30 的输入范围。
3. 09:10 新建 `shift-20261117` 工作负载，描述 `泵站17夜班复核.csv`。
4. 09:15 保存 `传感器17时间窗` 候选列 `sensor_id,recorded_at`。
5. 09:20 创建 `shift-20261117` 试验，worker 真正建索引和执行查询。
6. 09:25 查看 EXPLAIN、前缀评分、结果 SHA-256、写入探针。
7. 09:30 导出 `trial-<id>.json`，viewer 登录只读复核。
8. 10:00 admin 查看审计记录并检查执行完成事件。
9. 11:00 用「重采样对照」复核事故：选 `传感器17时间窗` 候选，偏斜 1,500 行聚焦传感器 17，提交后对比重新采样前后的计划、估计与实查行数。

种子含三账号、三负载、三候选与 5,000 遥测行。固定公式生成，不依赖随机数。seed 根据自然键/id补缺，不覆盖用户编辑。范围错误/非法列/重复名称有错误响应，空结果仍可正常比较。

## 算法与真实边界
Python 对索引从左到右分析，匹配相等列每列 +40，首个时间范围列 +20 后停止，最高100。缺失首列停止。启发式不等于优化器成本。worker 使用 MySQL GET_LOCK 串行隔离，删除遗留试验索引 → 无候选基线 EXPLAIN/查询5次 → 建候选索引/ANALYZE → 查询5次和EXPLAIN → 校验有序全结果SHA-256 → 清理索引。数据固定、查询参数固定可复现，耗时受缓存和驱动波动影响，不保证提速。20 行写探针是真实 INSERT 后 ROLLBACK，可能消耗 auto_increment 序列号。试验只修改专用 telemetry 表，不能接受任意SQL或远端数据库。任务使用简单 Redis 单消费者，进程崩溃后的 running 任务不自动重试，保留状态供人工复查并新建任务。

## 技术与目录
Python3.11 / FastAPI / SQLAlchemy / PyMySQL / Redis；MySQL8.0；原生 HTML CSS ES modules；nginx1.27。恰好五容器 frontend/backend/worker/db/redis，均有健康探针。数据库、Redis、应用产物各用命名卷。

`backend/src` 按 models/schemas/services/controllers/routes/middlewares/config/constants/utils 分层；`backend/scripts/seed_demo.py`；`backend/migrations` 记录元数据建表版本。`frontend/src` 按 api/stores/pages/components/common/hooks/router/types/constants/utils 分层。截图、测试脚本及测试报告在批次根目录 `test-artifacts/gb-4102`，不放源码；对照试验的回归脚本为 `python3 test-artifacts/gb-4102/test_comparison.py`（需后端环境变量与真实 MySQL）。

共享枚举：角色和状态在 `backend/src/constants/enums.py` 与 `frontend/src/constants/enums.js`；服务/权限与前端状态组件使用一致枚举。前端共享 table/status/feedback；hooks forms/poll。双字体来自 Google Fonts（网络不可用时回退 sans-serif/monospace）。赭红工业试验纸视觉，支持桌面/平板/手机。

环境变量 `.env.example`：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、JWT_SECRET、SEED_DEMO。容器内 DATABASE_URL/REDIS_URL 来自 Compose。SEED_DEMO启动失败仅记录日志；正常启动一次自然键补缺。全局错误处理/请求追踪中间件、JWT授权与业务审计横切。

本地开发：`pip install -r backend/requirements.txt`，设置 DATABASE_URL、REDIS_URL、JWT_SECRET 后在 backend 运行 `uvicorn src.main:app --reload`；前端需 nginx `/api/` 反向代理或开发代理，直接file://不能运行。许可 MIT。
