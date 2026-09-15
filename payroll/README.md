# 纸坊排班 · 考勤 · 计件工资系统

班组排班、考勤纠错与计件工资结算一体化系统。后端仅依赖 Python 标准库
(`http.server` + `sqlite3`),前端为单页应用,数据落盘 SQLite(WAL),重启保持。

## 运行

```bash
cd payroll
python3 server.py          # 默认端口 8090, 可用 PM_PORT / PM_DB 覆盖
```

访问 `http://localhost:8090`。数据保存在 `data/paper_mill.db`。

## 测试

```bash
python3 test_api.py        # 78 项端到端断言, 独立端口与临时库, 不影响正式数据
```

覆盖:排班冲突/休息不足拦截、打卡窗口、幂等重放、补卡证据+他人复核+重算、
变更链、完工幂等、结算归集、跨期产出不重复计提、封账只读、调整单对平、
并发打卡/结算原子性、失败不留半笔、重启持久化。

## 业务规则

| 模块 | 规则 |
|---|---|
| 排班 | 每班按工序排人;同一人时间窗不得重叠;跨班间隔 < 8 小时直接拦下(409) |
| 打卡 | 仅落在本人班次前 60 分钟 ~ 后 120 分钟宽限内,窗口外拒绝并留痕;`idem_key` 唯一,重放返回原记录不重复计工 |
| 补卡 | 必须提交证据;复核人不得为申请人;通过后生成 `MAKEUP` 打卡并按 `makeup:{id}` 幂等,随后自动重算工时 |
| 工时 | 有效工时 = 打卡对与班次窗口交集 + 加班 − 停工;请假/调出清零;缺卡记 0 并标记 MISSING |
| 变更 | 请假/加班/停工/调班全部写 `change_events`(seq + prev_id 链),调班自动生成新排班并复核冲突 |
| 完工 | `batch_key` 唯一,重放不重复登记 |
| 结算 | 按工序完工数量 × 计件单价 + 有效工时 × 计时单价归集;`settlement_sources` 唯一约束保证每条产出全系统只计提一次;每期间仅一张结算单 |
| 封账 | OPEN → SETTLED → CLOSED;封账后该期间排班/打卡/补卡/变更/完工一律 409,原单只读 |
| 更正 | 只能开调整单(ADJUSTMENT):服务端重算每行差额,`账面 + 调整 = 调整后` 与申报不符即 409;员工累计金额为负不予入账 |
| 事务 | 所有多写端点全局锁 + `BEGIN IMMEDIATE` 单事务,失败整体回滚,不留半笔 |

金额内部以「分」存储,界面显示元;计时金额按 分钟×时薪/60 四舍五入。

## API 一览

```
GET  /api/meta                          基础数据+规则参数
POST /api/workers|processes|templates|periods
POST /api/shifts                        开班 {date, template_id, team}
GET  /api/shifts?from=&to=              班次列表(含时间窗)
GET  /api/shifts/{id}                   班次+排班+考勤
POST /api/assignments                   排人 {shift_id, worker_id, process_id}
POST /api/punches                       打卡 {worker_id, ts, type, idem_key}
GET  /api/punches?worker_id=&date=
POST /api/makeup                        补卡申请 {worker_id, ts, type, evidence, requester_id}
POST /api/makeup/{id}/review            复核 {reviewer_id, approve, note}
GET  /api/makeup?status=
POST /api/changes                       变更 {assignment_id, type, payload, reason, by}
GET  /api/changes?assignment_id=        变更链
POST /api/outputs                       完工 {shift_id, process_id, worker_id, qty, batch_key}
GET  /api/outputs?shift_id=
GET  /api/attendance?date=|shift_id=    考勤工时
POST /api/periods/{id}/settle           结算
POST /api/periods/{id}/close            封账
POST /api/periods/{id}/adjust           调整单 {lines, expected_after, reason}
GET  /api/periods/{id}                  期间+单据+有效汇总
GET  /api/docs/{id}                     单据明细
```

页面八个页签与接口一一对应:基础数据、排班、打卡、补卡复核、变更链、完工、考勤、结算封账(含调整单)。
