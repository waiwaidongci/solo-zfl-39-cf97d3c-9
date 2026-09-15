#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端到端测试: 排班/打卡/补卡复核/变更链/完工/结算/封账/调整/并发/重启持久化"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 8391
BASE = "http://127.0.0.1:%d" % PORT
DB = os.path.join(tempfile.gettempdir(), "pm_test_%d.db" % os.getpid())

PASS = []


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, extra=""):
    if not cond:
        raise AssertionError("FAIL %s %s" % (name, extra))
    PASS.append(name)
    print("  PASS %s" % name)


def start_server():
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(DB + suffix):
            os.remove(DB + suffix)
    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "server.py")],
        env={**os.environ, "PM_PORT": str(PORT), "PM_DB": DB},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            code, _ = call("GET", "/api/meta")
            if code == 200:
                return proc
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError("server 启动失败")


def attendance_of(aid):
    code, rows = call("GET", "/api/attendance?date=2026-09-10")
    assert code == 200
    for r in rows:
        if r["assignment_id"] == aid:
            return r
    code, rows = call("GET", "/api/attendance?date=2026-09-11")
    for r in rows:
        if r["assignment_id"] == aid:
            return r
    raise AssertionError("attendance not found for %s" % aid)


def main():
    proc = start_server()
    try:
        run_tests()
        # ---------------- 重启持久化 ----------------
        print("== 重启持久化 ==")
        code, before = call("GET", "/api/periods/1")
        code, punches_before = call("GET", "/api/punches?date=2026-09-10")
        proc.terminate(); proc.wait()
        proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "server.py")],
            env={**os.environ, "PM_PORT": str(PORT), "PM_DB": DB},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                if call("GET", "/api/meta")[0] == 200:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        code, after = call("GET", "/api/periods/1")
        check("重启后期间有效总额保持", after["effective_total"] == before["effective_total"] == 240042,
              after.get("effective_total"))
        code, punches_after = call("GET", "/api/punches?date=2026-09-10")
        check("重启后打卡流水保持", len(punches_after) == len(punches_before) and len(punches_after) > 0)
        code, m = call("GET", "/api/meta")
        check("重启后基础数据保持", len(m["workers"]) == 8 and len(m["processes"]) == 4)
    finally:
        proc.terminate()
        proc.wait()
    print("\n全部通过: %d 项" % len(PASS))


def run_tests():
    print("== 基础数据 ==")
    code, m = call("GET", "/api/meta")
    check("种子数据", len(m["workers"]) == 8 and len(m["processes"]) == 4 and len(m["templates"]) == 3)
    W = {w["name"]: w["id"] for w in m["workers"]}
    P = {p["name"]: p["id"] for p in m["processes"]}
    T = {t["name"]: t["id"] for t in m["templates"]}
    w1, w2, w3, w4, w5 = W["王建国"], W["李秀兰"], W["张卫东"], W["赵春梅"], W["陈志强"]

    print("== 排班: 重叠与休息不足拦截 ==")
    shifts = {}
    for key, date, tpl, team in [("S1", "2026-09-10", "早班", "甲班"), ("S2", "2026-09-10", "中班", "甲班"),
                                 ("S3", "2026-09-10", "夜班", "甲班"), ("S4", "2026-09-10", "早班", "乙班"),
                                 ("S5", "2026-09-11", "早班", "甲班"), ("S6", "2026-09-11", "中班", "甲班")]:
        code, r = call("POST", "/api/shifts", {"date": date, "template_id": T[tpl], "team": team})
        check("开班 %s" % key, code == 201, r)
        shifts[key] = r["id"]
    code, r = call("POST", "/api/shifts", {"date": "2026-09-10", "template_id": T["早班"], "team": "甲班"})
    check("重复开班被拦", code == 409, r)

    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w1, "process_id": P["制浆"]})
    check("正常排班", code == 201, r)
    a1 = r["id"]
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S4"], "worker_id": w1, "process_id": P["制浆"]})
    check("同人重叠被拦", code == 409 and "重叠" in r["error"], r)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S2"], "worker_id": w1, "process_id": P["制浆"]})
    check("跨班休息不足被拦(间隔0)", code == 409 and "休息不足" in r["error"], r)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S3"], "worker_id": w1, "process_id": P["制浆"]})
    check("间隔正好8小时放行", code == 201, r)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S5"], "worker_id": w1, "process_id": P["制浆"]})
    check("夜班后接早班休息不足被拦", code == 409 and "休息不足" in r["error"], r)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w2, "process_id": P["抄纸"]})
    a2 = r["id"]; check("排班 w2", code == 201)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w3, "process_id": P["制浆"]})
    a3 = r["id"]; check("排班 w3", code == 201)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w4, "process_id": P["复卷"]})
    a4 = r["id"]; check("排班 w4", code == 201)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w2, "process_id": P["烘干"]})
    check("同班同人重复排被拦", code == 409, r)

    print("== 打卡: 班次窗口 + 幂等重放 ==")
    code, r = call("POST", "/api/punches", {"worker_id": w1, "type": "IN", "ts": "2026-09-10T05:50", "idem_key": "t-in-1"})
    check("上班打卡(宽限内)", code == 201 and r["status"] == "ACCEPTED", r)
    code, r = call("POST", "/api/punches", {"worker_id": w1, "type": "OUT", "ts": "2026-09-10T14:30", "idem_key": "t-out-1"})
    check("下班打卡", code == 201 and r["status"] == "ACCEPTED", r)
    att = attendance_of(a1)
    check("工时按班次窗口归集", att["base_minutes"] == 480 and att["att_status"] == "OK", att)
    code, r = call("POST", "/api/punches", {"worker_id": w1, "type": "IN", "ts": "2026-09-10T05:50", "idem_key": "t-in-1"})
    check("同一打卡重放返回原记录", code == 200 and r.get("replay") and r["status"] == "ACCEPTED", r)
    code, rows = call("GET", "/api/punches?worker_id=%d" % w1)
    check("重放不产生新记录", len([p for p in rows if p["idem_key"] == "t-in-1"]) == 1)
    att = attendance_of(a1)
    check("重放不重复计工", att["base_minutes"] == 480, att)
    code, r = call("POST", "/api/punches", {"worker_id": w2, "type": "IN", "ts": "2026-09-10T04:30", "idem_key": "t-bad-1"})
    check("班次窗口外打卡被拒", code == 201 and r["status"] == "REJECTED", r)
    code, r = call("POST", "/api/punches", {"worker_id": w2, "type": "IN", "ts": "2026-09-10T04:30", "idem_key": "t-bad-1"})
    check("被拒打卡重放仍返回原记录", code == 200 and r.get("replay") and r["status"] == "REJECTED", r)
    code, rows = call("GET", "/api/punches?worker_id=%d" % w2)
    check("被拒打卡只留一笔", len(rows) == 1, rows)
    code, r = call("POST", "/api/punches", {"worker_id": w3, "type": "IN", "ts": "2026-09-10T06:05", "idem_key": "t-in-3"})
    att = attendance_of(a3)
    check("漏卡状态与工时0", att["att_status"] == "MISSING" and att["valid_minutes"] == 0, att)

    print("== 补卡: 证据 + 他人复核 + 重算 ==")
    code, r = call("POST", "/api/makeup", {"worker_id": w3, "type": "OUT", "ts": "2026-09-10T14:10",
                                           "evidence": "", "requester_id": w2})
    check("无证据补卡被拦", code == 400, r)
    code, r = call("POST", "/api/makeup", {"worker_id": w3, "type": "OUT", "ts": "2026-09-10T14:10",
                                           "evidence": "车间监控截图+班长签字", "requester_id": w2})
    check("提交补卡申请", code == 201, r)
    mk = r["id"]
    code, r = call("POST", "/api/makeup/%d/review" % mk, {"reviewer_id": w2, "approve": True})
    check("申请人不得自审", code == 409 and "另一人" in r["error"], r)
    code, r = call("POST", "/api/makeup/%d/review" % mk, {"reviewer_id": w1, "approve": True, "note": "监控属实"})
    check("他人复核通过并补卡", code == 200 and r["status"] == "APPROVED" and r.get("punch_id"), r)
    att = attendance_of(a3)
    check("补卡后工时重算", att["att_status"] == "OK" and att["valid_minutes"] == 475, att)
    code, r = call("POST", "/api/makeup/%d/review" % mk, {"reviewer_id": w1, "approve": True})
    check("重复复核被拦", code == 409, r)
    code, r = call("POST", "/api/makeup", {"worker_id": w2, "type": "OUT", "ts": "2026-09-10T14:05",
                                           "evidence": "班长证明", "requester_id": w1})
    check("再挂一笔待复核(封账后用)", code == 201, r)
    mk_pending = r["id"]

    print("== 变更链: 请假/加班/停工/调班 ==")
    code, r = call("POST", "/api/changes", {"assignment_id": a1, "type": "OVERTIME",
                                            "payload": {"minutes": 30}, "reason": "赶订单", "by": "班长"})
    check("加班入账", code == 201 and r["seq"] == 1, r)
    att = attendance_of(a1)
    check("加班后工时", att["valid_minutes"] == 510, att)
    code, r = call("POST", "/api/changes", {"assignment_id": a1, "type": "SHUTDOWN",
                                            "payload": {"minutes": 60}, "reason": "断浆停机", "by": "班长"})
    check("停工入账", code == 201 and r["seq"] == 2, r)
    att = attendance_of(a1)
    check("停工后工时", att["valid_minutes"] == 450, att)
    code, chain = call("GET", "/api/changes?assignment_id=%d" % a1)
    check("变更链有序且相连", [c["seq"] for c in chain] == [1, 2] and chain[1]["prev_id"] == chain[0]["id"], chain)
    code, r = call("POST", "/api/changes", {"assignment_id": a2, "type": "LEAVE", "reason": "病假", "by": "班长"})
    check("请假入账", code == 201, r)
    att = attendance_of(a2)
    check("请假工时清零", att["att_status"] == "LEAVE" and att["valid_minutes"] == 0, att)
    code, r = call("POST", "/api/changes", {"assignment_id": a4, "type": "SWAP",
                                            "payload": {"to_shift_id": shifts["S6"]}, "reason": "家里有事", "by": "班长"})
    check("调班入账", code == 201 and r["payload"].get("new_assignment_id"), r)
    att = attendance_of(a4)
    check("调出后原班不计工", att["att_status"] == "SWAPPED_OUT" and att["valid_minutes"] == 0, att)
    code, r = call("POST", "/api/changes", {"assignment_id": a2, "type": "LEAVE", "reason": "重复"})
    check("非在岗状态不可再请假", code == 409, r)

    print("== 完工: 幂等 ==")
    code, r = call("POST", "/api/outputs", {"shift_id": shifts["S1"], "process_id": P["制浆"],
                                            "worker_id": w1, "qty": 10, "batch_key": "b-1"})
    check("完工登记", code == 201, r)
    code, r = call("POST", "/api/outputs", {"shift_id": shifts["S1"], "process_id": P["制浆"],
                                            "worker_id": w1, "qty": 10, "batch_key": "b-1"})
    check("完工重放返回原记录", code == 200 and r.get("replay"), r)
    code, rows = call("GET", "/api/outputs?shift_id=%d" % shifts["S1"])
    check("重放不重复计提", len(rows) == 1, rows)
    code, r = call("POST", "/api/outputs", {"shift_id": shifts["S1"], "process_id": P["制浆"],
                                            "worker_id": w3, "qty": 5, "batch_key": "b-2"})
    check("完工登记2", code == 201, r)
    code, r = call("POST", "/api/outputs", {"shift_id": shifts["S1"], "process_id": P["制浆"],
                                            "worker_id": w1, "qty": 0, "batch_key": "b-3"})
    check("数量非正被拦", code == 400, r)

    print("== 结算 ==")
    code, r = call("POST", "/api/periods/1/settle", {"by": "财务"})
    check("结算成功", code == 201 and r["total"] == 218542, r)
    doc1 = r["doc_id"]
    code, d = call("GET", "/api/docs/%d" % doc1)
    amts = {(l["worker_id"], l["process_id"]): l["amount"] for l in d["lines"]}
    check("w1 计件+计时", amts.get((w1, P["制浆"])) == 138750, amts)
    check("w3 计件+计时", amts.get((w3, P["制浆"])) == 79792, amts)
    check("请假/调出行为0", amts.get((w2, P["抄纸"])) == 0 and amts.get((w4, P["复卷"])) == 0, amts)
    code, r = call("POST", "/api/periods/1/settle", {"by": "财务"})
    check("重复结算被拦", code == 409, r)

    print("== 封账: 原单只读 ==")
    code, r = call("POST", "/api/periods/1/close", {"by": "财务"})
    check("封账", code == 200, r)
    code, r = call("POST", "/api/periods/1/close", {"by": "财务"})
    check("重复封账被拦", code == 409, r)
    code, r = call("POST", "/api/outputs", {"shift_id": shifts["S1"], "process_id": P["制浆"],
                                            "worker_id": w1, "qty": 1, "batch_key": "b-9"})
    check("封账后完工登记被拦", code == 409 and "封账" in r["error"], r)
    code, r = call("POST", "/api/assignments", {"shift_id": shifts["S1"], "worker_id": w5, "process_id": P["烘干"]})
    check("封账后排班被拦", code == 409, r)
    code, r = call("POST", "/api/changes", {"assignment_id": a3, "type": "OVERTIME", "payload": {"minutes": 30}})
    check("封账后变更被拦", code == 409, r)
    code, r = call("POST", "/api/makeup/%d/review" % mk_pending, {"reviewer_id": w3, "approve": True})
    check("封账后补卡复核被拦", code == 409, r)
    code, r = call("POST", "/api/punches", {"worker_id": w1, "type": "IN", "ts": "2026-09-10T05:55", "idem_key": "t-closed"})
    check("封账期间打卡被拒并留痕", code == 201 and r["status"] == "REJECTED" and "封账" in r["reason"], r)

    print("== 调整单: 差额对平 ==")
    code, r = call("POST", "/api/periods/1/adjust", {"lines": [{"worker_id": w1, "process_id": P["制浆"],
                     "qty_delta": 2, "minutes_delta": 0}], "expected_after": 999, "reason": "漏记2吨"})
    check("差额不平被拦", code == 409 and "不平" in r["error"], r)
    code, r = call("POST", "/api/periods/1/adjust", {"lines": [{"worker_id": w1, "process_id": P["制浆"],
                     "qty_delta": 2, "minutes_delta": 0}], "expected_after": 242542, "reason": "漏记2吨"})
    check("调整单入账且对平", code == 201 and r["before_total"] == 218542 and r["after_total"] == 242542, r)
    code, r = call("POST", "/api/periods/1/adjust", {"lines": [{"worker_id": w1, "minutes_delta": -60}],
                                                     "expected_after": 240042, "reason": "多计工时1小时"})
    check("工时负调整", code == 201 and r["delta"] == -2500 and r["after_total"] == 240042, r)
    code, r = call("POST", "/api/periods/1/adjust", {"lines": [{"worker_id": w3, "process_id": P["制浆"],
                     "qty_delta": -100}], "reason": "超额负调整"})
    check("调整后为负被拦", code == 409 and "为负" in r["error"], r)
    code, before_docs = call("GET", "/api/periods/1")
    n_docs = len(before_docs["docs"])
    code, r = call("POST", "/api/periods/1/adjust", {"lines": [
        {"worker_id": w1, "process_id": P["制浆"], "qty_delta": 1},
        {"worker_id": 99999, "process_id": P["制浆"], "qty_delta": 1}], "reason": "含坏行"})
    bad_code = code
    code, after_docs = call("GET", "/api/periods/1")
    check("失败调整不留半笔", bad_code in (400, 404, 409) and len(after_docs["docs"]) == n_docs, r)
    code, pd = call("GET", "/api/periods/1")
    eff = {(l["worker_id"], l["process_id"]): l for l in pd["effective_lines"]}
    check("有效总额=结算+调整", pd["effective_total"] == 240042, pd["effective_total"])
    check("有效行反映调整", eff[(w1, P["制浆"])]["qty"] == 12 and eff[(w1, None)]["minutes"] == -60
          and eff[(w1, None)]["hour_amount"] == -2500, eff)

    print("== 跨期产出不得重复计提 ==")
    code, r = call("POST", "/api/periods", {"name": "重叠期", "start_date": "2026-09-10", "end_date": "2026-09-11"})
    pid2 = r["id"]
    code, r = call("POST", "/api/periods/%d/settle" % pid2, {"by": "财务"})
    check("重叠期结算被拦(产出已计提)", code == 409, r)
    code, d2 = call("GET", "/api/periods/%d" % pid2)
    check("被拦结算不留半笔", d2["status"] == "OPEN" and len(d2["docs"]) == 0, d2)

    print("== 并发: 打卡幂等 + 结算原子 ==")
    code, r = call("POST", "/api/periods", {"name": "2026年10月", "start_date": "2026-10-01", "end_date": "2026-10-31"})
    pid3 = r["id"]
    code, r = call("POST", "/api/shifts", {"date": "2026-10-08", "template_id": T["早班"], "team": "甲班"})
    s7 = r["id"]
    code, r = call("POST", "/api/assignments", {"shift_id": s7, "worker_id": w5, "process_id": P["制浆"]})
    a5 = r["id"]
    check("10月排班", code == 201, r)

    results = []
    def punch_thread():
        results.append(call("POST", "/api/punches",
                            {"worker_id": w5, "type": "IN", "ts": "2026-10-08T05:55", "idem_key": "cc-1"}))
    threads = [threading.Thread(target=punch_thread) for _ in range(10)]
    [t.start() for t in threads]; [t.join() for t in threads]
    code, rows = call("GET", "/api/punches?worker_id=%d" % w5)
    check("并发同键打卡只记一笔", len([p for p in rows if p["idem_key"] == "cc-1"]) == 1,
          [p for p in rows if p["idem_key"] == "cc-1"])
    call("POST", "/api/punches", {"worker_id": w5, "type": "OUT", "ts": "2026-10-08T14:10", "idem_key": "cc-2"})
    call("POST", "/api/outputs", {"shift_id": s7, "process_id": P["制浆"], "worker_id": w5, "qty": 3, "batch_key": "cc-b1"})

    settle_res = []
    def settle_thread():
        settle_res.append(call("POST", "/api/periods/%d/settle" % pid3, {"by": "财务"}))
    threads = [threading.Thread(target=settle_thread) for _ in range(5)]
    [t.start() for t in threads]; [t.join() for t in threads]
    oks = [r for r in settle_res if r[0] == 201]
    check("并发结算只有一笔成功", len(oks) == 1 and oks[0][1]["total"] == 56000, settle_res)
    code, d3 = call("GET", "/api/periods/%d" % pid3)
    check("并发结算后只有一张结算单", len([d for d in d3["docs"] if d["type"] == "SETTLEMENT"]) == 1, d3["docs"])
    check("并发结算总额正确", d3["effective_total"] == 56000, d3["effective_total"])


if __name__ == "__main__":
    main()
