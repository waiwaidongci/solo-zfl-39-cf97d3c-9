#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
纸坊班组排班 · 考勤纠错 · 计件工资结算系统(后端,仅 Python 标准库)

核心保证:
- 排班:同人不得重叠;跨班休息不足(MIN_REST_MINUTES)直接拦下
- 打卡:只能落在本人班次附近(前后宽限);漏卡/补卡需证据+他人复核;补卡后重算工时
- 变更:请假/加班/停工/调班全部走 change_events 链(seq + prev_id)
- 幂等:打卡 idem_key、完工 batch_key 唯一,重放返回原单不重复计工
- 结算:按工序完工数量 + 有效工时归集;settlement_sources 保证每条产出只计提一次
- 封账:期间 CLOSED 后原单只读;更正只能开调整单;调整前后差额必须对平
- 事务:所有多写端点 BEGIN IMMEDIATE ... COMMIT/ROLLBACK,失败不留半笔
- 持久化:SQLite 文件(WAL),重启数据保持
"""
import json
import math
import os
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("PM_DB", os.path.join(BASE, "data", "paper_mill.db"))
PORT = int(os.environ.get("PM_PORT", "8090"))

MIN_REST_MINUTES = 480   # 跨班最小休息 8 小时
GRACE_BEFORE_MIN = 60    # 上班前 60 分钟内可打卡
GRACE_AFTER_MIN = 120    # 下班后 120 分钟内可打卡

LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  team TEXT NOT NULL,
  hour_rate INTEGER NOT NULL DEFAULT 2500,  -- 计时单价, 分/小时
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS processes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  unit TEXT NOT NULL DEFAULT '件',
  piece_rate INTEGER NOT NULL DEFAULT 0     -- 计件单价, 分/件
);
CREATE TABLE IF NOT EXISTS shift_templates(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  start_time TEXT NOT NULL,  -- HH:MM
  end_time TEXT NOT NULL     -- end<=start 视为跨零点
);
CREATE TABLE IF NOT EXISTS shifts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,        -- YYYY-MM-DD
  template_id INTEGER NOT NULL REFERENCES shift_templates,
  team TEXT NOT NULL,
  UNIQUE(date, template_id, team)
);
CREATE TABLE IF NOT EXISTS assignments(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shift_id INTEGER NOT NULL REFERENCES shifts,
  worker_id INTEGER NOT NULL REFERENCES workers,
  process_id INTEGER NOT NULL REFERENCES processes,
  status TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE/LEAVE/SWAPPED_OUT
  created_by TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(shift_id, worker_id)              -- 同班同人只排一个工序
);
CREATE TABLE IF NOT EXISTS change_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  assignment_id INTEGER NOT NULL REFERENCES assignments,
  seq INTEGER NOT NULL,                    -- 链内序号
  prev_id INTEGER,                         -- 上一条变更, 构成链
  type TEXT NOT NULL,                      -- LEAVE/OVERTIME/SHUTDOWN/SWAP
  payload TEXT NOT NULL DEFAULT '{}',
  reason TEXT,
  created_by TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(assignment_id, seq)
);
CREATE TABLE IF NOT EXISTS punches(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  idem_key TEXT UNIQUE NOT NULL,           -- 幂等键: 重放返回原记录
  worker_id INTEGER NOT NULL REFERENCES workers,
  ts TEXT NOT NULL,
  type TEXT NOT NULL,                      -- IN/OUT
  source TEXT NOT NULL DEFAULT 'NORMAL',   -- NORMAL/MAKEUP
  status TEXT NOT NULL,                    -- ACCEPTED/REJECTED
  reason TEXT,
  assignment_id INTEGER,
  shift_id INTEGER,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS makeup_requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL REFERENCES workers,
  ts TEXT NOT NULL,
  type TEXT NOT NULL,
  evidence TEXT NOT NULL,                  -- 补卡证据
  requester_id INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING/APPROVED/REJECTED
  reviewer_id INTEGER,
  review_note TEXT,
  punch_id INTEGER,
  created_at TEXT NOT NULL,
  reviewed_at TEXT
);
CREATE TABLE IF NOT EXISTS attendance_results(
  assignment_id INTEGER PRIMARY KEY REFERENCES assignments,
  in_ts TEXT, out_ts TEXT,
  base_minutes INTEGER NOT NULL DEFAULT 0,
  adjust_minutes INTEGER NOT NULL DEFAULT 0,  -- 加班+/停工-
  valid_minutes INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,                    -- OK/MISSING/LEAVE/SWAPPED_OUT
  version INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS output_records(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_key TEXT UNIQUE NOT NULL,          -- 幂等键: 同一批产出重放不重复
  shift_id INTEGER NOT NULL REFERENCES shifts,
  process_id INTEGER NOT NULL REFERENCES processes,
  worker_id INTEGER NOT NULL REFERENCES workers,
  qty INTEGER NOT NULL,
  recorded_by TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS periods(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'OPEN'      -- OPEN/SETTLED/CLOSED
);
CREATE TABLE IF NOT EXISTS payroll_docs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  period_id INTEGER NOT NULL REFERENCES periods,
  type TEXT NOT NULL,                      -- SETTLEMENT/ADJUSTMENT
  total INTEGER NOT NULL DEFAULT 0,        -- SETTLEMENT: 总额; ADJUSTMENT: 差额(可负)
  before_total INTEGER, after_total INTEGER,
  reason TEXT,
  ref_doc_id INTEGER,
  created_by TEXT,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_settlement_per_period
  ON payroll_docs(period_id) WHERE type='SETTLEMENT';
CREATE TABLE IF NOT EXISTS payroll_lines(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER NOT NULL REFERENCES payroll_docs,
  worker_id INTEGER NOT NULL,
  process_id INTEGER,
  qty INTEGER NOT NULL DEFAULT 0,
  minutes INTEGER NOT NULL DEFAULT 0,
  piece_amount INTEGER NOT NULL DEFAULT 0,
  hour_amount INTEGER NOT NULL DEFAULT 0,
  amount INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS settlement_sources(
  output_record_id INTEGER PRIMARY KEY REFERENCES output_records,  -- 每条产出最多计提一次
  doc_id INTEGER NOT NULL REFERENCES payroll_docs
);
"""


class BizError(Exception):
    def __init__(self, msg, code=400):
        super().__init__(msg)
        self.code = code


def now_iso():
    return datetime.now().replace(microsecond=0).isoformat()


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


DB = get_conn()
DB.executescript(SCHEMA)
DB.commit()


def tx(fn):
    """写操作包装: 全局锁串行 + 单事务, 失败整体回滚, 不留半笔。"""
    def wrapper(*a, **kw):
        with LOCK:
            DB.execute("BEGIN IMMEDIATE")
            try:
                r = fn(*a, **kw)
                DB.commit()
                return r
            except BizError:
                DB.rollback()
                raise
            except sqlite3.IntegrityError as e:
                DB.rollback()
                raise BizError("数据约束冲突: %s" % e, 409)
            except Exception:
                DB.rollback()
                raise
    return wrapper


def q(sql, args=()):
    with LOCK:
        return DB.execute(sql, args).fetchall()


def q1(sql, args=()):
    with LOCK:
        return DB.execute(sql, args).fetchone()


def parse_dt(s):
    try:
        return datetime.fromisoformat(str(s).strip())
    except Exception:
        raise BizError("时间格式不正确: %r (应为 YYYY-MM-DDTHH:MM)" % s)


def parse_date(s):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(s or "")):
        raise BizError("日期格式不正确: %r (应为 YYYY-MM-DD)" % s)
    return str(s)


def round_half(x):
    """四舍五入(半进), 与前端 Math.round 一致。"""
    return math.floor(x + 0.5)


def hour_amount(minutes, rate):
    return round_half(minutes * rate / 60.0)


def shift_window(row):
    """row 需含 date/start_time/end_time; end<=start 跨零点 +1 天。"""
    start = datetime.fromisoformat(row["date"] + "T" + row["start_time"])
    end = datetime.fromisoformat(row["date"] + "T" + row["end_time"])
    if end <= start:
        end += timedelta(days=1)
    return start, end


def closed_period_for(date_str):
    return q1("SELECT id,name FROM periods WHERE status='CLOSED' AND start_date<=? AND end_date>=?",
              (date_str, date_str))


def assert_not_closed(date_str):
    p = closed_period_for(date_str)
    if p:
        raise BizError("期间「%s」已封账, 原单只读, 更正请走调整单" % p["name"], 409)


def overlapping_periods(start_date, end_date, exclude_id=None):
    """日期范围有交集即重叠(共享任意一天都算); 相邻不重叠。"""
    sql = "SELECT id,name,start_date,end_date FROM periods WHERE start_date<=? AND end_date>=?"
    args = [end_date, start_date]
    if exclude_id is not None:
        sql += " AND id<>?"
        args.append(exclude_id)
    return q(sql, args)


# ---------------------------------------------------------------- 排班

def check_worker_available(worker_id, new_start, new_end, exclude_assignment_id=None):
    rows = q("""SELECT a.id aid, s.date, t.start_time, t.end_time
                FROM assignments a
                JOIN shifts s ON s.id=a.shift_id
                JOIN shift_templates t ON t.id=s.template_id
                WHERE a.worker_id=? AND a.status='ACTIVE'""", (worker_id,))
    for r in rows:
        if exclude_assignment_id and r["aid"] == exclude_assignment_id:
            continue
        s, e = shift_window(r)
        if s < new_end and new_start < e:
            raise BizError("排班冲突: 该员工 %s %s-%s 已有班次, 同一人不得重叠"
                           % (r["date"], r["start_time"], r["end_time"]), 409)
        gap = (new_start - e).total_seconds() / 60 if new_start >= e \
            else (s - new_end).total_seconds() / 60
        if gap < MIN_REST_MINUTES:
            raise BizError("跨班休息不足: 与 %s %s-%s 班间隔仅 %d 分钟, 不足 %d 分钟"
                           % (r["date"], r["start_time"], r["end_time"], int(gap), MIN_REST_MINUTES), 409)


@tx
def create_shift(body):
    date = parse_date(body.get("date"))
    tpl = q1("SELECT * FROM shift_templates WHERE id=?", (body.get("template_id"),))
    if not tpl:
        raise BizError("班次模板不存在", 404)
    team = (body.get("team") or "").strip()
    if not team:
        raise BizError("班组不能为空")
    assert_not_closed(date)
    cur = DB.execute("INSERT INTO shifts(date,template_id,team) VALUES(?,?,?)",
                     (date, tpl["id"], team))
    return 201, {"id": cur.lastrowid}


@tx
def create_assignment(body):
    shift = q1("""SELECT s.*, t.start_time, t.end_time FROM shifts s
                  JOIN shift_templates t ON t.id=s.template_id WHERE s.id=?""",
               (body.get("shift_id"),))
    if not shift:
        raise BizError("班次不存在", 404)
    worker = q1("SELECT * FROM workers WHERE id=? AND active=1", (body.get("worker_id"),))
    if not worker:
        raise BizError("员工不存在或已停用", 404)
    proc = q1("SELECT * FROM processes WHERE id=?", (body.get("process_id"),))
    if not proc:
        raise BizError("工序不存在", 404)
    assert_not_closed(shift["date"])
    s, e = shift_window(shift)
    check_worker_available(worker["id"], s, e)
    cur = DB.execute(
        "INSERT INTO assignments(shift_id,worker_id,process_id,created_by,created_at) VALUES(?,?,?,?,?)",
        (shift["id"], worker["id"], proc["id"], body.get("by") or "system", now_iso()))
    aid = cur.lastrowid
    recompute_assignment(aid)
    return 201, {"id": aid}


# ---------------------------------------------------------------- 考勤重算

def recompute_assignment(aid):
    a = q1("""SELECT a.*, s.date, t.start_time, t.end_time FROM assignments a
              JOIN shifts s ON s.id=a.shift_id
              JOIN shift_templates t ON t.id=s.template_id WHERE a.id=?""", (aid,))
    if not a:
        return
    s, e = shift_window(a)
    punches = q("SELECT * FROM punches WHERE assignment_id=? AND status='ACCEPTED' ORDER BY ts", (aid,))
    ins = [p for p in punches if p["type"] == "IN"]
    outs = [p for p in punches if p["type"] == "OUT"]
    in_ts = out_ts = None
    if a["status"] == "LEAVE":
        base, st = 0, "LEAVE"
    elif a["status"] == "SWAPPED_OUT":
        base, st = 0, "SWAPPED_OUT"
    elif not ins or not outs:
        base, st = 0, "MISSING"
    else:
        in_dt = min(parse_dt(p["ts"]) for p in ins)
        out_dt = max(parse_dt(p["ts"]) for p in outs)
        if out_dt <= in_dt:
            base, st = 0, "MISSING"
        else:
            lo, hi = max(in_dt, s), min(out_dt, e)
            base = max(0, int((hi - lo).total_seconds() // 60))
            st = "OK"
            in_ts, out_ts = in_dt.isoformat(), out_dt.isoformat()
    adj = 0
    for ev in q("SELECT type,payload FROM change_events WHERE assignment_id=?" , (aid,)):
        p = json.loads(ev["payload"])
        if ev["type"] == "OVERTIME":
            adj += int(p.get("minutes", 0))
        elif ev["type"] == "SHUTDOWN":
            adj -= int(p.get("minutes", 0))
    valid = max(0, base + adj)
    DB.execute("""INSERT INTO attendance_results
                  (assignment_id,in_ts,out_ts,base_minutes,adjust_minutes,valid_minutes,status,version,updated_at)
                  VALUES(?,?,?,?,?,?,?,1,?)
                  ON CONFLICT(assignment_id) DO UPDATE SET
                    in_ts=excluded.in_ts, out_ts=excluded.out_ts,
                    base_minutes=excluded.base_minutes, adjust_minutes=excluded.adjust_minutes,
                    valid_minutes=excluded.valid_minutes, status=excluded.status,
                    version=attendance_results.version+1, updated_at=excluded.updated_at""",
               (aid, in_ts, out_ts, base, adj, valid, st, now_iso()))


# ---------------------------------------------------------------- 打卡

def find_assignment_for_ts(worker_id, ts):
    rows = q("""SELECT a.id aid, a.shift_id, s.date, t.start_time, t.end_time
                FROM assignments a
                JOIN shifts s ON s.id=a.shift_id
                JOIN shift_templates t ON t.id=s.template_id
                WHERE a.worker_id=? AND a.status='ACTIVE'""", (worker_id,))
    best = None
    for r in rows:
        s, e = shift_window(r)
        lo = s - timedelta(minutes=GRACE_BEFORE_MIN)
        hi = e + timedelta(minutes=GRACE_AFTER_MIN)
        if lo <= ts <= hi:
            d = 0 if s <= ts <= e else min(abs((ts - s).total_seconds()), abs((ts - e).total_seconds()))
            if best is None or d < best[0]:
                best = (d, r)
    return best[1] if best else None


def insert_punch(worker_id, ts, ptype, idem_key, source):
    """在事务内调用: 幂等 + 班次窗口校验 + 重算。返回 (status_code, punch_row_dict)。"""
    existing = q1("SELECT * FROM punches WHERE idem_key=?", (idem_key,))
    if existing:  # 同一打卡重放: 返回原记录, 不重复计工
        return 200, dict(existing, replay=True)
    worker = q1("SELECT * FROM workers WHERE id=? AND active=1", (worker_id,))
    if not worker:
        raise BizError("员工不存在或已停用", 404)
    m = find_assignment_for_ts(worker_id, ts)
    if m and closed_period_for(m["date"]):
        status, reason, aid, sid = "REJECTED", "该班次所在期间已封账", None, None
    elif m:
        status, reason, aid, sid = "ACCEPTED", None, m["aid"], m["shift_id"]
    else:
        status, reason, aid, sid = "REJECTED", "打卡时间不在本人任何班次附近(前后宽限内)", None, None
    cur = DB.execute("""INSERT INTO punches(idem_key,worker_id,ts,type,source,status,reason,assignment_id,shift_id,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                     (idem_key, worker_id, ts.isoformat(), ptype, source, status, reason, aid, sid, now_iso()))
    if aid:
        recompute_assignment(aid)
    return 201, dict(q1("SELECT * FROM punches WHERE id=?", (cur.lastrowid,)))


@tx
def post_punch(body):
    ptype = body.get("type")
    if ptype not in ("IN", "OUT"):
        raise BizError("type 必须为 IN 或 OUT")
    ts = parse_dt(body.get("ts"))
    idem_key = (body.get("idem_key") or "").strip() or str(uuid.uuid4())
    return insert_punch(body.get("worker_id"), ts, ptype, idem_key, "NORMAL")


# ---------------------------------------------------------------- 补卡与复核

@tx
def post_makeup(body):
    worker = q1("SELECT * FROM workers WHERE id=? AND active=1", (body.get("worker_id"),))
    if not worker:
        raise BizError("员工不存在或已停用", 404)
    if body.get("type") not in ("IN", "OUT"):
        raise BizError("type 必须为 IN 或 OUT")
    ts = parse_dt(body.get("ts"))
    evidence = (body.get("evidence") or "").strip()
    if not evidence:
        raise BizError("补卡必须提交证据")
    requester_id = body.get("requester_id")
    if not q1("SELECT id FROM workers WHERE id=?", (requester_id,)):
        raise BizError("申请人不存在", 404)
    cur = DB.execute("""INSERT INTO makeup_requests(worker_id,ts,type,evidence,requester_id,created_at)
                        VALUES(?,?,?,?,?,?)""",
                     (worker["id"], ts.isoformat(), body["type"], evidence, requester_id, now_iso()))
    return 201, {"id": cur.lastrowid}


@tx
def review_makeup(mid, body):
    req = q1("SELECT * FROM makeup_requests WHERE id=?", (mid,))
    if not req:
        raise BizError("补卡申请不存在", 404)
    if req["status"] != "PENDING":
        raise BizError("该申请已复核, 当前状态 %s" % req["status"], 409)
    reviewer_id = body.get("reviewer_id")
    if not q1("SELECT id FROM workers WHERE id=? AND active=1", (reviewer_id,)):
        raise BizError("复核人不存在", 404)
    if reviewer_id == req["requester_id"] or reviewer_id == req["worker_id"]:
        raise BizError("补卡必须由另一人复核: 复核人不得为申请人或被补卡员工本人", 409)
    note = body.get("note") or ""
    if not body.get("approve"):
        DB.execute("UPDATE makeup_requests SET status='REJECTED',reviewer_id=?,review_note=?,reviewed_at=? WHERE id=?",
                   (reviewer_id, note, now_iso(), mid))
        return 200, {"id": mid, "status": "REJECTED"}
    # 通过: 创建补卡打卡(幂等键=makeup:id), 随后重算工时
    m = find_assignment_for_ts(req["worker_id"], parse_dt(req["ts"]))
    if m and closed_period_for(m["date"]):
        raise BizError("该班次所在期间已封账, 请走调整单更正", 409)
    if not m:
        raise BizError("补卡时间不在本人任何班次附近, 无法通过", 409)
    code, punch = insert_punch(req["worker_id"], parse_dt(req["ts"]), req["type"],
                               "makeup:%d" % mid, "MAKEUP")
    if punch["status"] != "ACCEPTED":
        raise BizError("补卡未生效: %s" % (punch.get("reason") or "未知原因"), 409)
    DB.execute("""UPDATE makeup_requests SET status='APPROVED',reviewer_id=?,review_note=?,punch_id=?,reviewed_at=?
                  WHERE id=?""",
               (reviewer_id, note, punch["id"], now_iso(), mid))
    return 200, {"id": mid, "status": "APPROVED", "punch_id": punch["id"]}


# ---------------------------------------------------------------- 变更链

@tx
def post_change(body):
    a = q1("""SELECT a.*, s.date, t.start_time, t.end_time FROM assignments a
              JOIN shifts s ON s.id=a.shift_id
              JOIN shift_templates t ON t.id=s.template_id WHERE a.id=?""",
           (body.get("assignment_id"),))
    if not a:
        raise BizError("排班明细不存在", 404)
    ctype = body.get("type")
    if ctype not in ("LEAVE", "OVERTIME", "SHUTDOWN", "SWAP"):
        raise BizError("变更类型必须为 LEAVE/OVERTIME/SHUTDOWN/SWAP")
    assert_not_closed(a["date"])
    payload = dict(body.get("payload") or {})
    if ctype in ("OVERTIME", "SHUTDOWN"):
        minutes = int(payload.get("minutes") or 0)
        if minutes <= 0 or minutes > 720:
            raise BizError("分钟数必须为 1-720")
        payload["minutes"] = minutes
    if ctype == "LEAVE" and a["status"] != "ACTIVE":
        raise BizError("当前状态 %s 不可请假" % a["status"], 409)
    if ctype == "SWAP":
        if a["status"] != "ACTIVE":
            raise BizError("当前状态 %s 不可调班" % a["status"], 409)
        target = q1("""SELECT s.*, t.start_time, t.end_time FROM shifts s
                       JOIN shift_templates t ON t.id=s.template_id WHERE s.id=?""",
                    (payload.get("to_shift_id"),))
        if not target:
            raise BizError("目标班次不存在", 404)
        assert_not_closed(target["date"])
        new_proc = int(payload.get("process_id") or a["process_id"])
        if not q1("SELECT id FROM processes WHERE id=?", (new_proc,)):
            raise BizError("工序不存在", 404)
        s2, e2 = shift_window(target)
        check_worker_available(a["worker_id"], s2, e2, exclude_assignment_id=a["id"])
        DB.execute("UPDATE assignments SET status='SWAPPED_OUT' WHERE id=?", (a["id"],))
        cur = DB.execute("""INSERT INTO assignments(shift_id,worker_id,process_id,created_by,created_at)
                            VALUES(?,?,?,?,?)""",
                         (target["id"], a["worker_id"], new_proc, body.get("by") or "system", now_iso()))
        payload["new_assignment_id"] = cur.lastrowid
        payload["to_shift_id"] = target["id"]
    if ctype == "LEAVE":
        DB.execute("UPDATE assignments SET status='LEAVE' WHERE id=?", (a["id"],))
    last = q1("SELECT id,seq FROM change_events WHERE assignment_id=? ORDER BY seq DESC LIMIT 1", (a["id"],))
    seq = (last["seq"] + 1) if last else 1
    cur = DB.execute("""INSERT INTO change_events(assignment_id,seq,prev_id,type,payload,reason,created_by,created_at)
                        VALUES(?,?,?,?,?,?,?,?)""",
                     (a["id"], seq, last["id"] if last else None, ctype,
                      json.dumps(payload, ensure_ascii=False), body.get("reason") or "",
                      body.get("by") or "system", now_iso()))
    recompute_assignment(a["id"])
    if payload.get("new_assignment_id"):
        recompute_assignment(payload["new_assignment_id"])
    return 201, {"id": cur.lastrowid, "seq": seq, "payload": payload}


# ---------------------------------------------------------------- 完工

@tx
def post_output(body):
    batch_key = (body.get("batch_key") or "").strip() or str(uuid.uuid4())
    existing = q1("SELECT * FROM output_records WHERE batch_key=?", (batch_key,))
    if existing:  # 同一批产出重放: 返回原记录, 不重复计提
        return 200, dict(existing, replay=True)
    shift = q1("SELECT * FROM shifts WHERE id=?", (body.get("shift_id"),))
    if not shift:
        raise BizError("班次不存在", 404)
    assert_not_closed(shift["date"])
    if not q1("SELECT id FROM processes WHERE id=?", (body.get("process_id"),)):
        raise BizError("工序不存在", 404)
    if not q1("SELECT id FROM workers WHERE id=? AND active=1", (body.get("worker_id"),)):
        raise BizError("员工不存在或已停用", 404)
    asg = q1("SELECT id,status FROM assignments WHERE shift_id=? AND worker_id=? AND process_id=?",
             (shift["id"], body.get("worker_id"), body.get("process_id")))
    if not asg:
        raise BizError("该员工未在此班次的此工序排班, 产出不能入账", 409)
    if asg["status"] != "ACTIVE":
        raise BizError("该员工此班次排班状态为 %s, 产出不能入账" % asg["status"], 409)
    qty = int(body.get("qty") or 0)
    if qty <= 0:
        raise BizError("完工数量必须为正整数")
    cur = DB.execute("""INSERT INTO output_records(batch_key,shift_id,process_id,worker_id,qty,recorded_by,created_at)
                        VALUES(?,?,?,?,?,?,?)""",
                     (batch_key, shift["id"], body["process_id"], body["worker_id"], qty,
                      body.get("by") or "system", now_iso()))
    return 201, dict(q1("SELECT * FROM output_records WHERE id=?", (cur.lastrowid,)))


# ---------------------------------------------------------------- 结算 / 封账 / 调整

def period_or_404(pid):
    p = q1("SELECT * FROM periods WHERE id=?", (pid,))
    if not p:
        raise BizError("结算期间不存在", 404)
    return p


def shift_ids_in_period(p):
    return [r["id"] for r in q("SELECT id FROM shifts WHERE date BETWEEN ? AND ?",
                               (p["start_date"], p["end_date"]))]


@tx
def settle(pid, body):
    p = period_or_404(pid)
    if p["status"] != "OPEN":
        raise BizError("期间状态为 %s, 不能结算(仅 OPEN 可结算)" % p["status"], 409)
    ov = overlapping_periods(p["start_date"], p["end_date"], exclude_id=pid)
    if ov:
        raise BizError("期间边界与期间「%s」(%s ~ %s)重叠, 同一份有效工时会被重复归集, 请先更正边界再结算"
                       % (ov[0]["name"], ov[0]["start_date"], ov[0]["end_date"]), 409)
    sids = shift_ids_in_period(p)
    marks = ",".join("?" * len(sids)) or "NULL"
    att = q("""SELECT a.worker_id, a.process_id, SUM(ar.valid_minutes) m
               FROM assignments a JOIN attendance_results ar ON ar.assignment_id=a.id
               WHERE a.shift_id IN (%s) GROUP BY a.worker_id, a.process_id""" % marks, sids)
    outs = q("SELECT * FROM output_records WHERE shift_id IN (%s)" % marks, sids)
    missing = q("""SELECT a.id FROM assignments a JOIN attendance_results ar ON ar.assignment_id=a.id
                   WHERE a.shift_id IN (%s) AND ar.status='MISSING'""" % marks, sids)
    agg = {}
    for r in att:
        agg[(r["worker_id"], r["process_id"])] = {"minutes": r["m"] or 0, "qty": 0}
    for o in outs:
        k = (o["worker_id"], o["process_id"])
        agg.setdefault(k, {"minutes": 0, "qty": 0})
        agg[k]["qty"] += o["qty"]
    cur = DB.execute("""INSERT INTO payroll_docs(period_id,type,total,created_by,created_at)
                        VALUES(?,'SETTLEMENT',0,?,?)""",
                     (pid, body.get("by") or "system", now_iso()))
    doc_id = cur.lastrowid
    total = 0
    for (wid, proc_id), v in sorted(agg.items()):
        w = q1("SELECT * FROM workers WHERE id=?", (wid,))
        pr = q1("SELECT * FROM processes WHERE id=?", (proc_id,))
        piece = v["qty"] * (pr["piece_rate"] if pr else 0)
        hour = hour_amount(v["minutes"], w["hour_rate"] if w else 0)
        amount = piece + hour
        DB.execute("""INSERT INTO payroll_lines(doc_id,worker_id,process_id,qty,minutes,piece_amount,hour_amount,amount)
                      VALUES(?,?,?,?,?,?,?,?)""",
                   (doc_id, wid, proc_id, v["qty"], v["minutes"], piece, hour, amount))
        total += amount
    for o in outs:  # 每条产出登记来源, 唯一约束保证跨班/跨期不重复计提
        DB.execute("INSERT INTO settlement_sources(output_record_id,doc_id) VALUES(?,?)", (o["id"], doc_id))
    DB.execute("UPDATE payroll_docs SET total=? WHERE id=?", (total, doc_id))
    DB.execute("UPDATE periods SET status='SETTLED' WHERE id=?", (pid,))
    return 201, {"doc_id": doc_id, "total": total,
                 "warnings": ["存在 %d 条缺卡考勤(工时按 0 计)" % len(missing)] if missing else []}


@tx
def close_period(pid, body):
    p = period_or_404(pid)
    if p["status"] == "CLOSED":
        raise BizError("期间已封账", 409)
    if p["status"] != "SETTLED":
        raise BizError("期间尚未结算, 不能封账", 409)
    DB.execute("UPDATE periods SET status='CLOSED' WHERE id=?", (pid,))
    return 200, {"id": pid, "status": "CLOSED"}


def effective_total(pid):
    r = q1("SELECT COALESCE(SUM(total),0) t FROM payroll_docs WHERE period_id=?", (pid,))
    return r["t"]


def effective_worker_totals(pid):
    rows = q("""SELECT l.worker_id, SUM(l.amount) a FROM payroll_lines l
                JOIN payroll_docs d ON d.id=l.doc_id WHERE d.period_id=? GROUP BY l.worker_id""", (pid,))
    return {r["worker_id"]: r["a"] for r in rows}


@tx
def adjust(pid, body):
    p = period_or_404(pid)
    if p["status"] != "CLOSED":
        raise BizError("仅封账后的期间可开调整单(当前 %s)" % p["status"], 409)
    settle_doc = q1("SELECT * FROM payroll_docs WHERE period_id=? AND type='SETTLEMENT'", (pid,))
    lines = body.get("lines") or []
    if not lines:
        raise BizError("调整单至少一行")
    computed = []
    for i, ln in enumerate(lines, 1):
        w = q1("SELECT * FROM workers WHERE id=?", (ln.get("worker_id"),))
        if not w:
            raise BizError("第 %d 行: 员工不存在" % i)
        qty_d = int(ln.get("qty_delta") or 0)
        min_d = int(ln.get("minutes_delta") or 0)
        if qty_d == 0 and min_d == 0:
            raise BizError("第 %d 行: 数量与工时差额不能同时为 0" % i)
        proc_id = ln.get("process_id")
        piece = 0
        if qty_d:
            if not proc_id:
                raise BizError("第 %d 行: 数量调整必须指定工序" % i)
            pr = q1("SELECT * FROM processes WHERE id=?", (proc_id,))
            if not pr:
                raise BizError("第 %d 行: 工序不存在" % i)
            piece = qty_d * pr["piece_rate"]
        hour = hour_amount(min_d, w["hour_rate"])
        computed.append({"worker_id": w["id"], "process_id": proc_id, "qty": qty_d,
                         "minutes": min_d, "piece_amount": piece, "hour_amount": hour,
                         "amount": piece + hour})
    delta = sum(c["amount"] for c in computed)
    before = effective_total(pid)
    after = before + delta
    expected = body.get("expected_after")
    if expected is not None and int(expected) != after:
        raise BizError("调整前后差额不平: 账面 %d + 调整 %d = %d, 与申报 %s 不符"
                       % (before, delta, after, expected), 409)
    per_worker = effective_worker_totals(pid)
    for c in computed:
        per_worker[c["worker_id"]] = per_worker.get(c["worker_id"], 0) + c["amount"]
    neg = [wid for wid, amt in per_worker.items() if amt < 0]
    if neg:
        raise BizError("调整后员工 %s 累计金额为负, 不予入账" % neg, 409)
    cur = DB.execute("""INSERT INTO payroll_docs(period_id,type,total,before_total,after_total,reason,ref_doc_id,created_by,created_at)
                        VALUES(?,'ADJUSTMENT',?,?,?,?,?,?,?)""",
                     (pid, delta, before, after, body.get("reason") or "",
                      settle_doc["id"], body.get("by") or "system", now_iso()))
    doc_id = cur.lastrowid
    for c in computed:
        DB.execute("""INSERT INTO payroll_lines(doc_id,worker_id,process_id,qty,minutes,piece_amount,hour_amount,amount)
                      VALUES(?,?,?,?,?,?,?,?)""",
                   (doc_id, c["worker_id"], c["process_id"], c["qty"], c["minutes"],
                    c["piece_amount"], c["hour_amount"], c["amount"]))
    return 201, {"doc_id": doc_id, "before_total": before, "delta": delta, "after_total": after}


# ---------------------------------------------------------------- 查询

def get_meta():
    return {
        "workers": [dict(r) for r in q("SELECT * FROM workers ORDER BY team,id")],
        "processes": [dict(r) for r in q("SELECT * FROM processes ORDER BY id")],
        "templates": [dict(r) for r in q("SELECT * FROM shift_templates ORDER BY id")],
        "periods": [dict(r) for r in q("SELECT * FROM periods ORDER BY start_date DESC")],
        "rules": {"min_rest_minutes": MIN_REST_MINUTES,
                  "grace_before_min": GRACE_BEFORE_MIN, "grace_after_min": GRACE_AFTER_MIN},
    }


def get_shifts(params):
    date_from = params.get("from") or "0000-01-01"
    date_to = params.get("to") or "9999-12-31"
    rows = q("""SELECT s.*, t.name template_name, t.start_time, t.end_time,
                (SELECT COUNT(*) FROM assignments a WHERE a.shift_id=s.id) headcount
                FROM shifts s JOIN shift_templates t ON t.id=s.template_id
                WHERE s.date BETWEEN ? AND ? ORDER BY s.date, t.start_time, s.team""",
             (date_from, date_to))
    out = []
    for r in rows:
        d = dict(r)
        s, e = shift_window(r)
        d["window"] = [s.isoformat(), e.isoformat()]
        out.append(d)
    return out


def get_shift_detail(sid):
    s = q1("""SELECT s.*, t.name template_name, t.start_time, t.end_time FROM shifts s
              JOIN shift_templates t ON t.id=s.template_id WHERE s.id=?""", (sid,))
    if not s:
        raise BizError("班次不存在", 404)
    d = dict(s)
    w0, w1 = shift_window(s)
    d["window"] = [w0.isoformat(), w1.isoformat()]
    d["assignments"] = [dict(r) for r in q("""
        SELECT a.*, w.name worker_name, w.team worker_team, p.name process_name,
               ar.in_ts, ar.out_ts, ar.base_minutes, ar.adjust_minutes, ar.valid_minutes,
               COALESCE(ar.status,'NONE') att_status
        FROM assignments a
        JOIN workers w ON w.id=a.worker_id
        JOIN processes p ON p.id=a.process_id
        LEFT JOIN attendance_results ar ON ar.assignment_id=a.id
        WHERE a.shift_id=? ORDER BY a.id""", (sid,))]
    return d


def get_punches(params):
    sql = """SELECT p.*, w.name worker_name FROM punches p JOIN workers w ON w.id=p.worker_id"""
    args, cond = [], []
    if params.get("worker_id"):
        cond.append("p.worker_id=?"); args.append(int(params["worker_id"]))
    if params.get("date"):
        cond.append("substr(p.ts,1,10)=?"); args.append(params["date"])
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    sql += " ORDER BY p.id DESC LIMIT 300"
    return [dict(r) for r in q(sql, args)]


def get_makeups(params):
    sql = """SELECT m.*, w.name worker_name, rq.name requester_name, rv.name reviewer_name
             FROM makeup_requests m
             JOIN workers w ON w.id=m.worker_id
             LEFT JOIN workers rq ON rq.id=m.requester_id
             LEFT JOIN workers rv ON rv.id=m.reviewer_id"""
    if params.get("status"):
        sql += " WHERE m.status=?"
        rows = q(sql + " ORDER BY m.id DESC", (params["status"],))
    else:
        rows = q(sql + " ORDER BY m.id DESC")
    return [dict(r) for r in rows]


def get_changes(params):
    aid = params.get("assignment_id")
    if not aid:
        return [dict(r) for r in q("SELECT * FROM change_events ORDER BY id DESC LIMIT 200")]
    return [dict(r) for r in q("SELECT * FROM change_events WHERE assignment_id=? ORDER BY seq", (aid,))]


def get_outputs(params):
    sql = """SELECT o.*, w.name worker_name, p.name process_name, s.date shift_date
             FROM output_records o
             JOIN workers w ON w.id=o.worker_id
             JOIN processes p ON p.id=o.process_id
             JOIN shifts s ON s.id=o.shift_id"""
    args, cond = [], []
    if params.get("shift_id"):
        cond.append("o.shift_id=?"); args.append(int(params["shift_id"]))
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    return [dict(r) for r in q(sql + " ORDER BY o.id DESC LIMIT 300", args)]


def get_attendance(params):
    cond, args = "", []
    if params.get("shift_id"):
        cond, args = "WHERE a.shift_id=?", [int(params["shift_id"])]
    elif params.get("date"):
        cond, args = "WHERE s.date=?", [params["date"]]
    return [dict(r) for r in q("""
        SELECT a.id assignment_id, a.status assign_status, w.name worker_name, w.team,
               p.name process_name, s.date, t.name template_name, t.start_time, t.end_time,
               ar.in_ts, ar.out_ts, ar.base_minutes, ar.adjust_minutes, ar.valid_minutes,
               COALESCE(ar.status,'NONE') att_status, ar.version
        FROM assignments a
        JOIN workers w ON w.id=a.worker_id
        JOIN processes p ON p.id=a.process_id
        JOIN shifts s ON s.id=a.shift_id
        JOIN shift_templates t ON t.id=s.template_id
        LEFT JOIN attendance_results ar ON ar.assignment_id=a.id
        %s ORDER BY s.date, a.id""" % cond, args)]


def get_period_detail(pid):
    p = period_or_404(pid)
    d = dict(p)
    d["docs"] = [dict(r) for r in q("SELECT * FROM payroll_docs WHERE period_id=? ORDER BY id", (pid,))]
    d["effective_total"] = effective_total(pid)
    rows = q("""SELECT l.worker_id, w.name worker_name, l.process_id, pr.name process_name,
                       SUM(l.qty) qty, SUM(l.minutes) minutes,
                       SUM(l.piece_amount) piece_amount, SUM(l.hour_amount) hour_amount, SUM(l.amount) amount
                FROM payroll_lines l JOIN payroll_docs doc ON doc.id=l.doc_id
                JOIN workers w ON w.id=l.worker_id
                LEFT JOIN processes pr ON pr.id=l.process_id
                WHERE doc.period_id=? GROUP BY l.worker_id, l.process_id ORDER BY l.worker_id, l.process_id""",
             (pid,))
    d["effective_lines"] = [dict(r) for r in rows]
    return d


def get_doc(doc_id):
    d = q1("SELECT * FROM payroll_docs WHERE id=?", (doc_id,))
    if not d:
        raise BizError("单据不存在", 404)
    out = dict(d)
    out["lines"] = [dict(r) for r in q("""
        SELECT l.*, w.name worker_name, p.name process_name FROM payroll_lines l
        JOIN workers w ON w.id=l.worker_id LEFT JOIN processes p ON p.id=l.process_id
        WHERE l.doc_id=? ORDER BY l.id""", (doc_id,))]
    return out


# ---------------------------------------------------------------- 种子数据

def seed():
    if q1("SELECT id FROM workers LIMIT 1"):
        return
    with LOCK:
        DB.execute("BEGIN IMMEDIATE")
        try:
            for name, team in [("王建国", "甲班"), ("李秀兰", "甲班"), ("张卫东", "甲班"), ("赵春梅", "甲班"),
                               ("陈志强", "乙班"), ("刘桂芳", "乙班"), ("孙明辉", "乙班"), ("周雅琴", "乙班")]:
                DB.execute("INSERT INTO workers(name,team,hour_rate) VALUES(?,?,2500)", (name, team))
            for code, name, unit, rate in [("ZJ", "制浆", "吨", 12000), ("CZ", "抄纸", "令", 15000),
                                           ("HG", "烘干", "令", 10000), ("FJ", "复卷", "卷", 8000)]:
                DB.execute("INSERT INTO processes(code,name,unit,piece_rate) VALUES(?,?,?,?)",
                           (code, name, unit, rate))
            for name, s, e in [("早班", "06:00", "14:00"), ("中班", "14:00", "22:00"), ("夜班", "22:00", "06:00")]:
                DB.execute("INSERT INTO shift_templates(name,start_time,end_time) VALUES(?,?,?)", (name, s, e))
            DB.execute("INSERT INTO periods(name,start_date,end_date) VALUES('2026年9月','2026-09-01','2026-09-30')")
            DB.commit()
        except Exception:
            DB.rollback()
            raise


seed()


# ---------------------------------------------------------------- HTTP 层

def route(method, path, params, body):
    if method == "GET" and path == "/api/meta":
        return 200, get_meta()
    if method == "GET" and path == "/api/shifts":
        return 200, get_shifts(params)
    m = re.fullmatch(r"/api/shifts/(\d+)", path)
    if m and method == "GET":
        return 200, get_shift_detail(int(m.group(1)))
    if method == "POST" and path == "/api/shifts":
        return create_shift(body)
    if method == "POST" and path == "/api/assignments":
        return create_assignment(body)
    if method == "POST" and path == "/api/punches":
        return post_punch(body)
    if method == "GET" and path == "/api/punches":
        return 200, get_punches(params)
    if method == "POST" and path == "/api/makeup":
        return post_makeup(body)
    if method == "GET" and path == "/api/makeup":
        return 200, get_makeups(params)
    m = re.fullmatch(r"/api/makeup/(\d+)/review", path)
    if m and method == "POST":
        return review_makeup(int(m.group(1)), body)
    if method == "POST" and path == "/api/changes":
        return post_change(body)
    if method == "GET" and path == "/api/changes":
        return 200, get_changes(params)
    if method == "POST" and path == "/api/outputs":
        return post_output(body)
    if method == "GET" and path == "/api/outputs":
        return 200, get_outputs(params)
    if method == "GET" and path == "/api/attendance":
        return 200, get_attendance(params)
    if method == "POST" and path == "/api/periods":
        return create_period(body)
    if method == "GET" and path == "/api/periods":
        return 200, [dict(r) for r in q("SELECT * FROM periods ORDER BY start_date DESC")]
    m = re.fullmatch(r"/api/periods/(\d+)", path)
    if m and method == "GET":
        return 200, get_period_detail(int(m.group(1)))
    m = re.fullmatch(r"/api/periods/(\d+)/settle", path)
    if m and method == "POST":
        return settle(int(m.group(1)), body)
    m = re.fullmatch(r"/api/periods/(\d+)/close", path)
    if m and method == "POST":
        return close_period(int(m.group(1)), body)
    m = re.fullmatch(r"/api/periods/(\d+)/adjust", path)
    if m and method == "POST":
        return adjust(int(m.group(1)), body)
    m = re.fullmatch(r"/api/docs/(\d+)", path)
    if m and method == "GET":
        return 200, get_doc(int(m.group(1)))
    if method == "POST" and path == "/api/workers":
        return create_worker(body)
    if method == "POST" and path == "/api/processes":
        return create_process(body)
    if method == "POST" and path == "/api/templates":
        return create_template(body)
    raise BizError("not_found", 404)


@tx
def create_period(body):
    name = (body.get("name") or "").strip()
    if not name:
        raise BizError("期间名称不能为空")
    s, e = parse_date(body.get("start_date")), parse_date(body.get("end_date"))
    if e < s:
        raise BizError("结束日期不能早于开始日期")
    ov = overlapping_periods(s, e)
    if ov:
        raise BizError("期间边界与已有期间「%s」(%s ~ %s)重叠, 同一份工时不允许落入两个期间"
                       % (ov[0]["name"], ov[0]["start_date"], ov[0]["end_date"]), 409)
    cur = DB.execute("INSERT INTO periods(name,start_date,end_date) VALUES(?,?,?)", (name, s, e))
    return 201, {"id": cur.lastrowid}


@tx
def create_worker(body):
    name = (body.get("name") or "").strip()
    team = (body.get("team") or "").strip()
    if not name or not team:
        raise BizError("姓名与班组不能为空")
    rate = int(body.get("hour_rate") or 2500)
    cur = DB.execute("INSERT INTO workers(name,team,hour_rate) VALUES(?,?,?)", (name, team, rate))
    return 201, {"id": cur.lastrowid}


@tx
def create_process(body):
    code = (body.get("code") or "").strip().upper()
    name = (body.get("name") or "").strip()
    if not code or not name:
        raise BizError("工序编码与名称不能为空")
    cur = DB.execute("INSERT INTO processes(code,name,unit,piece_rate) VALUES(?,?,?,?)",
                     (code, name, (body.get("unit") or "件").strip(), int(body.get("piece_rate") or 0)))
    return 201, {"id": cur.lastrowid}


@tx
def create_template(body):
    name = (body.get("name") or "").strip()
    st, et = (body.get("start_time") or ""), (body.get("end_time") or "")
    if not name or not re.fullmatch(r"\d{2}:\d{2}", st) or not re.fullmatch(r"\d{2}:\d{2}", et):
        raise BizError("模板名称与起止时间(HH:MM)不能为空")
    cur = DB.execute("INSERT INTO shift_templates(name,start_time,end_time) VALUES(?,?,?)", (name, st, et))
    return 201, {"id": cur.lastrowid}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _dispatch(self, method):
        try:
            from urllib.parse import urlparse, parse_qs
            u = urlparse(self.path)
            params = {k: v[0] for k, v in parse_qs(u.query).items()}
            body = {}
            if method == "POST":
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                body = json.loads(raw.decode("utf-8")) if raw else {}
            code, obj = route(method, u.path, params, body)
            self._send(code, obj)
        except BizError as e:
            self._send(e.code, {"error": str(e)})
        except Exception as e:
            self._send(500, {"error": "服务器内部错误: %s" % e})

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index.html"):
            try:
                with open(os.path.join(BASE, "static", "index.html"), "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self._send(404, {"error": "static/index.html 不存在"})
            return
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("纸坊排班考勤计件系统 listening on http://localhost:%d (db: %s)" % (PORT, DB_PATH))
    server.serve_forever()


if __name__ == "__main__":
    main()
