import sys, os, time, json, shutil, tempfile
from collections import deque
from datetime import datetime, timedelta
import pandas as pd
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer

CONFIG = {
    "BASE_ROOT": r"C:\Users\HONG\Desktop\구글드라이브(집)\데이터",
    "LOCAL_WORK_ROOT": r"C:\KiwoomCache\작업폴더",
    "START_DATE": "20260814",
    "LOG_LEVEL": "INFO",
    "FILTER_PREFERRED": True,
    "FILTER_ETF": True,
    "TARGET_CODES": [],
    "MIN_MCAP": 500,
    "SAFETY_OVERLAP_DAYS": 2,
    "REQUEST_INTERVAL_SEC": 0.05,
    "SECOND_SAFE_MAX": 10,
    "MINUTE_SAFE_MAX": 200,
    "SESSION_TR_LIMIT": 950,
    "REQUEST_TIMEOUT_MS": 45000,
    "MAX_COMM_RETRIES": 3,
    "MAX_TR_RETRY_PER_CODE": 2,
    "MAX_PAGES_PER_DATA": 40,
    "MAX_LOGIN_RETRIES": 3,
    "SAVE_INTERVAL": 10,
    "SCREEN_NO_BASE": 1000,
    "API_PAUSE_ON_ERROR": True,
    "AUTO_RESET_ON_ERROR": True, # ⭐ 오류시 자동 초기화
    "ERROR_THRESHOLD": 5, # 연속 5회 오류시 자동 초기화
}

BASE_ROOT = CONFIG["BASE_ROOT"]
LOCAL_ROOT = CONFIG["LOCAL_WORK_ROOT"]
START_DATE = CONFIG["START_DATE"]
RUN_DATE = datetime.now().strftime("%Y%m%d")
STATE_FILE = os.path.join(LOCAL_ROOT, "state.json")
LOG_FILE = os.path.join(LOCAL_ROOT, f"로그기록_{RUN_DATE}.txt")

os.makedirs(LOCAL_ROOT, exist_ok=True)
log_fp = open(LOG_FILE, "a", encoding="utf-8")

def log(level, msg):
    order = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}
    if order.get(level, 1) >= order.get(CONFIG["LOG_LEVEL"], 1):
        line = f"[{level}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        print(line)
        try:
            log_fp.write(line + "\n")
            log_fp.flush()
        except: pass

def auto_reset_today_state():
    """⭐ 오류 발생시 오늘 날짜 초기화"""
    try:
        if not os.path.exists(STATE_FILE):
            return False
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        reset_count = 0
        for k in list(state.keys()):
            if k == "_meta":
                continue
            if isinstance(state[k], dict) and state[k].get("last_date") == RUN_DATE:
                del state[k]["last_date"]
                reset_count += 1
        if reset_count > 0:
            atomic_write_json(STATE_FILE, state)
            log("WARN", f"🔄 자동 초기화 완료 - {reset_count}개 last_date 제거 (오류로 인한)")
            return True
        return False
    except Exception as e:
        log("ERROR", f"자동 초기화 실패 {e}")
        return False

def reset_single_code(code):
    """특정 코드만 초기화"""
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        if code in state and isinstance(state[code], dict):
            state[code].pop("last_date", None)
            atomic_write_json(STATE_FILE, state)
            log("INFO", f"🔄 {code} 단일 초기화")
    except: pass

def is_preferred_stock(code, name):
    if not CONFIG["FILTER_PREFERRED"]: return False
    return bool(name and (name.endswith("우") or "우B" in name or "우C" in name))

def is_derivative_product(name):
    if not name: return False
    exclude_keywords = ["ETN", "ETF", "선물", "옵션", "인버스", "레버리지", "KODEX", "TIGER", "KBSTAR", "RISE", "KOSEF", "3X", "2X", "쌍방향"]
    return any(k in name for k in exclude_keywords)

def parse_abs_int(value):
    raw = str(value or "").strip().replace(",", "")
    if not raw: return 0
    try: return abs(int(raw))
    except:
        try: return abs(int(float(raw)))
        except: return 0

def atomic_write_json(path, payload):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".state_", suffix=".tmp", dir=os.path.dirname(path))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_path, path)
        return True
    except: return False

def load_state():
    if not os.path.exists(STATE_FILE): return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def get_start_date_for_code(state, code, default_start):
    item = state.get(code, {})
    if not isinstance(item, dict) or not item.get("last_date"): return default_start
    try:
        last = datetime.strptime(str(item["last_date"]), "%Y%m%d")
        overlap = last - timedelta(days=CONFIG["SAFETY_OVERLAP_DAYS"])
        return max(default_start, overlap.strftime("%Y%m%d"))
    except: return default_start

def normalize_key_columns(df):
    out = df.copy()
    if "code" in out.columns:
        code = out["code"].astype(str).str.strip()
        mask = code.str.fullmatch(r"\d+", na=False)
        out.loc[mask, "code"] = code.loc[mask].str.zfill(6)
    if "time" in out.columns:
        clock = out["time"].astype(str).str.strip()
        mask = clock.str.fullmatch(r"\d+", na=False)
        out.loc[mask, "time"] = clock.loc[mask].str.zfill(6)
    return out

def read_csv_preserve_keys(path):
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        return normalize_key_columns(df)
    except: return pd.DataFrame()

def atomic_to_csv(df, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".csv_", suffix=".tmp", dir=os.path.dirname(path))
        os.close(fd)
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        os.replace(tmp_path, path)
        return True
    except: return False

class KiwoomRateLimited(QMainWindow):
    def __init__(self):
        super().__init__()
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.kiwoom.OnEventConnect.connect(self._on_connect)
        self.kiwoom.OnReceiveTrData.connect(self._on_tr_data)
        self.kiwoom.OnReceiveMsg.connect(self._on_msg)
        self.loop = None
        self._timer = None
        self.tr_ok = False
        self.prev_next = "0"
        self.reached_fetch_start = False
        self.active_rqname = ""
        self.active_screen_no = ""
        self.active_code = ""
        self.current_code = ""
        self.current_name = ""
        self.current_sector = ""
        self.current_mcap_group = ""
        self.current_mcap_value = 0
        self.current_fetch_start = START_DATE
        self.is_connected = False
        self.login_retry_count = 0
        self.pipeline_started = False
        self.total_tr_count = 0
        self.session_tr_count = 0
        self.stop_requested = False
        self.stop_reason = ""
        self.tr_timestamps = deque()
        self.api_paused = False
        self.pause_until = 0
        self.consecutive_errors = 0
        self.records = {
            "500to1000_daily": [], "Over1000_daily": [],
            "500to1000_3min": [], "Over1000_3min": [],
            "500to1000_5min": [], "Over1000_5min": [],
        }
        self.state = load_state()
        if "_meta" in self.state and isinstance(self.state["_meta"], dict):
            self.total_tr_count = self.state["_meta"].get("tr_count", 0)
        log("INFO", f"🚀 공격적 모드 + 자동초기화 ON - 로그: {LOG_FILE}")
        self.connect_with_retry()

    def connect_with_retry(self):
        log("INFO", "키움 연결 시도...")
        self.is_connected = False
        self.kiwoom.dynamicCall("CommConnect()")

    def _on_connect(self, err_code):
        if err_code!= 0:
            self.is_connected = False
            self.login_retry_count += 1
            log("ERROR", f"로그인 실패 {err_code} 재시도 {self.login_retry_count}")
            if self.login_retry_count >= CONFIG["MAX_LOGIN_RETRIES"]:
                if CONFIG["AUTO_RESET_ON_ERROR"]:
                    log("WARN", "로그인 실패 연속 - 자동 초기화 시도")
                    auto_reset_today_state()
                QApplication.quit()
                return
            QTimer.singleShot(10000, self.connect_with_retry)
            return
        log("INFO", "로그인 성공")
        self.is_connected = True
        self.login_retry_count = 0
        self.consecutive_errors = 0
        QTimer.singleShot(800, self.start_pipeline)

    def _on_msg(self, scr_no, rqname, trcode, msg):
        if any(k in msg for k in ["초과", "제한", "과다"]):
            log("WARN", f"🛑 API 정지 신호: {msg}")
            if CONFIG["API_PAUSE_ON_ERROR"]:
                self.api_paused = True
                self.pause_until = time.time() + 60
                log("INFO", f"60초 대기 후 자동 재개...")
        if any(k in msg for k in ["조회", "요청", "초과", "제한"]):
            log("WARN", f"키움메시지 [{scr_no}] {rqname} {msg}")

    def wait_tr(self):
        self.loop = QEventLoop()
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self.loop.quit)
        self._timer.start(CONFIG["REQUEST_TIMEOUT_MS"])
        self.loop.exec_()
        ok = self.tr_ok
        try:
            if self._timer: self._timer.stop()
        except: pass
        self.loop = None
        self._timer = None
        return ok

    def enforce_rate_limit(self):
        if self.api_paused:
            now = time.time()
            if now < self.pause_until:
                wait_sec = self.pause_until - now
                log("WARN", f"⏸ API 정지 중 - {wait_sec:.1f}초 대기...")
                time.sleep(wait_sec)
            else:
                self.api_paused = False
                log("INFO", f"✅ API 정지 해제 - 수집 재개")
        now = time.time()
        while self.tr_timestamps and now - self.tr_timestamps[0] > 60:
            self.tr_timestamps.popleft()
        recent_1s = sum(1 for t in self.tr_timestamps if now - t <= 1.0)
        if recent_1s >= CONFIG["SECOND_SAFE_MAX"]:
            time.sleep(0.1)
        if len(self.tr_timestamps) >= CONFIG["MINUTE_SAFE_MAX"]:
            sleep_sec = 60 - (now - self.tr_timestamps[0]) + 0.5
            if sleep_sec > 0:
                log("WARN", f"분당 제한 도달 - {sleep_sec:.1f}초 대기")
                time.sleep(sleep_sec)

    def request_tr_fixed(self, rqname, trcode, prev_next, screen_no):
        self.enforce_rate_limit()
        time.sleep(CONFIG["REQUEST_INTERVAL_SEC"])
        self.prev_next = "0"
        self.tr_ok = False
        self.reached_fetch_start = False
        self.active_rqname = rqname
        self.active_screen_no = screen_no
        self.active_code = self.current_code
        for _ in range(CONFIG["MAX_COMM_RETRIES"]):
            ret = self.kiwoom.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, int(prev_next), screen_no)
            if ret == 0: break
            time.sleep(0.5)
        else:
            self.consecutive_errors += 1
            log("ERROR", f"CommRqData 실패 연속 {self.consecutive_errors}회 - {rqname}")
            if CONFIG["AUTO_RESET_ON_ERROR"] and self.consecutive_errors >= CONFIG["ERROR_THRESHOLD"]:
                log("WARN", f"🔄 연속 오류 {self.consecutive_errors}회 - 자동 초기화 실행")
                auto_reset_today_state()
                self.consecutive_errors = 0
            return False
        ok = self.wait_tr()
        if ok:
            self.consecutive_errors = 0
            self.total_tr_count += 1
            self.session_tr_count += 1
            self.tr_timestamps.append(time.time())
            if "_meta" not in self.state or not isinstance(self.state["_meta"], dict):
                self.state["_meta"] = {}
            self.state["_meta"]["tr_count"] = self.total_tr_count
            if self.session_tr_count >= CONFIG["SESSION_TR_LIMIT"]:
                self.stop_requested = True
                self.stop_reason = f"세션 {CONFIG['SESSION_TR_LIMIT']}건"
        else:
            self.consecutive_errors += 1
            log("ERROR", f"TR 타임아웃 연속 {self.consecutive_errors}회 - {rqname} {self.current_code}")
            if CONFIG["AUTO_RESET_ON_ERROR"] and self.consecutive_errors >= CONFIG["ERROR_THRESHOLD"]:
                log("WARN", f"🔄 연속 타임아웃 {self.consecutive_errors}회 - {self.current_code} 초기화 후 재시도")
                reset_single_code(self.current_code)
                self.consecutive_errors = 0
        return ok

    def get_all_codes(self):
        if CONFIG["TARGET_CODES"]:
            return [c.strip() for c in CONFIG["TARGET_CODES"] if c.strip()]
        kospi = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "0") or ""
        kosdaq = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "10") or ""
        elw = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "3") or ""
        mf = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "4") or ""
        warrant = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "5") or ""
        reits = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "6") or ""
        etf = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "8") or ""
        hy = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "9") or ""
        exclude_set = set()
        for s in [elw, mf, warrant, reits, etf, hy]:
            exclude_set.update([c.strip() for c in s.split(";") if c.strip()])
        codes = [c.strip() for c in (kospi + ";" + kosdaq).split(";") if c.strip()]
        filtered = [c for c in codes if c not in exclude_set and len(c)==6 and c.isdigit()]
        final_filtered = []
        for code in filtered:
            try:
                name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code).strip()
                if not is_derivative_product(name) and not is_preferred_stock(code, name):
                    final_filtered.append(code)
            except:
                final_filtered.append(code)
        log("INFO", f"원본 {len(codes)}개 -> 순수주식 {len(filtered)}개 -> 최종 {len(final_filtered)}개")
        return final_filtered

    def _get_comm_data(self, trcode, record_name, idx, field):
        try:
            v = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, record_name, idx, field)
            return str(v).strip() if v is not None else ""
        except: return ""

    def get_basic_info_and_classify(self, code):
        self.current_code = code
        rqname = f"B_{code}"
        screen_no = f"{CONFIG['SCREEN_NO_BASE']+int(code[-4:])%800}"
        for _ in range(CONFIG["MAX_TR_RETRY_PER_CODE"]):
            self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
            ok = self.request_tr_fixed(rqname, "opt10001", "0", screen_no)
            if ok: return self.current_mcap_group
        return "Timeout"

    def get_daily(self, code, fetch_start):
        self.current_code = code
        self.current_fetch_start = fetch_start
        rqname = f"D_{code}"
        screen_no = f"{CONFIG['SCREEN_NO_BASE']+int(code[-4:])%800}"
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "기준일자", RUN_DATE)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        prev = "0"
        pages = 0
        while pages < CONFIG["MAX_PAGES_PER_DATA"]:
            ok = self.request_tr_fixed(rqname, "opt10081", prev, screen_no)
            if not ok: return False
            pages += 1
            if self.reached_fetch_start or self.prev_next!= "2": break
            prev = "2"
        return True

    def get_min(self, code, tick, fetch_start):
        self.current_code = code
        self.current_fetch_start = fetch_start
        rqname = f"M{tick}_{code}"
        screen_no = f"{CONFIG['SCREEN_NO_BASE']+int(code[-4:])%800}"
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "틱범위", str(tick))
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        prev = "0"
        pages = 0
        while pages < CONFIG["MAX_PAGES_PER_DATA"]:
            ok = self.request_tr_fixed(rqname, "opt10080", prev, screen_no)
            if not ok: return False
            pages += 1
            if self.reached_fetch_start or self.prev_next!= "2": break
            prev = "2"
        return True

    def _on_tr_data(self, scr_no, rqname, trcode, record_name, prev_next):
        if rqname!= self.active_rqname or scr_no!= self.active_screen_no: return
        try:
            self.prev_next = prev_next
            try: count = int(self.kiwoom.dynamicCall("GetRepeatCnt(QString, QString)", trcode, record_name) or 0)
            except: count = 0
            code = self.active_code or self.current_code
            try: name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code).strip()
            except: name = ""
            self.current_name = name
            if is_preferred_stock(code, name):
                self.current_mcap_group = "Preferred"
                self.tr_ok = True
                return
            if rqname.startswith("B_"):
                mcap_str = self._get_comm_data(trcode, record_name, 0, "시가총액").replace(",", "").replace("+", "").replace("-", "")
                try: mcap = int(mcap_str) if mcap_str else 0
                except: mcap = 0
                self.current_mcap_value = mcap
                self.current_sector = self._get_comm_data(trcode, record_name, 0, "업종")
                if mcap >= 1000: self.current_mcap_group = "Over1000"
                elif mcap >= CONFIG["MIN_MCAP"]: self.current_mcap_group = "500to1000"
                else: self.current_mcap_group = "Under500"
            elif rqname.startswith("D_"):
                if self.current_mcap_group not in ("Over1000", "500to1000"): self.tr_ok = True; return
                for idx in range(count):
                    date = self._get_comm_data(trcode, record_name, idx, "일자")
                    if not date: continue
                    if date <= self.current_fetch_start: self.reached_fetch_start = True
                    if date < self.current_fetch_start: continue
                    self.records[f"{self.current_mcap_group}_daily"].append({
                        "date": date, "ym": date[:6], "year": date[:4], "code": code, "name": name, "sector": self.current_sector,
                        "market_cap_억": self.current_mcap_value,
                        "open": self._get_comm_data(trcode, record_name, idx, "시가"),
                        "high": self._get_comm_data(trcode, record_name, idx, "고가"),
                        "low": self._get_comm_data(trcode, record_name, idx, "저가"),
                        "close": parse_abs_int(self._get_comm_data(trcode, record_name, idx, "현재가")),
                        "volume": self._get_comm_data(trcode, record_name, idx, "거래량"),
                        "amount": self._get_comm_data(trcode, record_name, idx, "거래대금"),
                    })
            elif rqname.startswith("M3_") or rqname.startswith("M5_"):
                if self.current_mcap_group not in ("Over1000", "500to1000"): self.tr_ok = True; return
                for idx in range(count):
                    raw_date = self._get_comm_data(trcode, record_name, idx, "일자")
                    raw_time = self._get_comm_data(trcode, record_name, idx, "체결시간")
                    if raw_time and len(raw_time) >= 14: date_part, time_part = raw_time[:8], raw_time[8:14]
                    else: date_part = raw_date or ""; time_part = self._get_comm_data(trcode, record_name, idx, "시간") or raw_time or ""
                    if not date_part: continue
                    if date_part <= self.current_fetch_start: self.reached_fetch_start = True
                    if date_part < self.current_fetch_start: continue
                    rec = {"date": date_part, "ym": date_part[:6], "year": date_part[:4], "time": time_part, "code": code, "name": name,
                           "open": self._get_comm_data(trcode, record_name, idx, "시가"),
                           "high": self._get_comm_data(trcode, record_name, idx, "고가"),
                           "low": self._get_comm_data(trcode, record_name, idx, "저가"),
                           "close": parse_abs_int(self._get_comm_data(trcode, record_name, idx, "현재가")),
                           "volume": self._get_comm_data(trcode, record_name, idx, "거래량"),
                           "amount": self._get_comm_data(trcode, record_name, idx, "거래대금") or "0"}
                    if rqname.startswith("M3_"): self.records[f"{self.current_mcap_group}_3min"].append(rec)
                    else: self.records[f"{self.current_mcap_group}_5min"].append(rec)
            self.tr_ok = True
        except Exception as e:
            log("ERROR", f"TR 오류 [{rqname}]: {e}")
            self.tr_ok = False
            if CONFIG["AUTO_RESET_ON_ERROR"]:
                log("WARN", f"TR 처리 오류 - {code} 초기화")
                reset_single_code(code)
        finally:
            try:
                if self.loop and self.loop.isRunning(): self.loop.quit()
            except: pass

    def sync_drive_to_local(self):
        try:
            os.makedirs(BASE_ROOT, exist_ok=True)
            os.makedirs(LOCAL_ROOT, exist_ok=True)
            for year in os.listdir(BASE_ROOT):
                yp = os.path.join(BASE_ROOT, year)
                if not os.path.isdir(yp): continue
                lp = os.path.join(LOCAL_ROOT, year)
                os.makedirs(lp, exist_ok=True)
                for fname in os.listdir(yp):
                    if not fname.endswith(".csv"): continue
                    src = os.path.join(yp, fname)
                    dst = os.path.join(lp, fname)
                    if not os.path.exists(dst):
                        try: shutil.copy2(src, dst)
                        except: pass
        except: pass

    def save_incremental_to_local(self):
        try:
            for group in ["500to1000", "Over1000"]:
                prefix = "1000억이상" if group == "Over1000" else "500억이상"
                for key, suffix, dedup in [(f"{group}_daily", "_일봉.csv", ["date", "code"]), (f"{group}_3min", "_3분봉.csv", ["date", "time", "code"]), (f"{group}_5min", "_5분봉.csv", ["date", "time", "code"])]:
                    if not self.records.get(key): continue
                    df = pd.DataFrame(self.records[key])
                    if df.empty: continue
                    for (year, ym), g in df.groupby(["year", "ym"]):
                        lp = os.path.join(LOCAL_ROOT, year)
                        os.makedirs(lp, exist_ok=True)
                        path = os.path.join(lp, f"{prefix}{ym}{suffix}")
                        self.merge_save(g.drop(columns=["ym", "year"], errors="ignore"), path, dedup)
            for k in self.records: self.records[k] = []
            return True
        except Exception as e:
            log("ERROR", f"save_incremental 실패 {e}")
            return False

    def final_replace_to_drive(self):
        try:
            for year in os.listdir(LOCAL_ROOT):
                lp = os.path.join(LOCAL_ROOT, year)
                if not os.path.isdir(lp) or year.startswith("."): continue
                dp = os.path.join(BASE_ROOT, year)
                os.makedirs(dp, exist_ok=True)
                for fname in os.listdir(lp):
                    if not fname.endswith(".csv"): continue
                    src = os.path.join(lp, fname)
                    dst = os.path.join(dp, fname)
                    try:
                        df = read_csv_preserve_keys(src)
                        if df.empty: continue
                        if "_일봉" in fname: df = df.drop_duplicates(subset=["date", "code"], keep="last")
                        else: df = df.drop_duplicates(subset=["date", "time", "code"], keep="last")
                        atomic_to_csv(df, dst)
                    except Exception as e:
                        log("ERROR", f"final_replace 실패 {fname} {e}")
        except Exception as e:
            log("ERROR", f"final_replace 전체 실패 {e}")

    def merge_save(self, new_df, path, dedup_keys):
        try:
            new_df = normalize_key_columns(new_df)
            if os.path.exists(path):
                old_df = read_csv_preserve_keys(path)
                if not old_df.empty:
                    combined = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(subset=dedup_keys, keep="last")
                else:
                    combined = new_df
            else:
                combined = new_df
            atomic_to_csv(combined, path)
        except Exception as e:
            log("ERROR", f"merge_save 실패 {path} {e}")

    def flush_checkpoint(self):
        try:
            ok_csv = self.save_incremental_to_local()
            ok_state = atomic_write_json(STATE_FILE, self.state)
            log("INFO", f"체크포인트 저장 CSV={ok_csv} STATE={ok_state} TR={self.session_tr_count}")
            return ok_csv and ok_state
        except Exception as e:
            log("ERROR", f"flush 실패 {e}")
            return False

    def checkpoint_and_finish(self, msg):
        if self.flush_checkpoint():
            self.final_replace_to_drive()
        log("INFO", msg)
        try:
            log_fp.close()
        except: pass

    def start_pipeline(self):
        if self.pipeline_started: return
        self.pipeline_started = True
        try:
            os.makedirs(BASE_ROOT, exist_ok=True)
            os.makedirs(LOCAL_ROOT, exist_ok=True)
            self.sync_drive_to_local()
            filtered = self.get_all_codes()
            to_do = []
            skipped = 0
            for c in filtered:
                item = self.state.get(c, {})
                if isinstance(item, dict) and item.get("last_date") == RUN_DATE and item.get("mcap_group") in ("Over1000", "500to1000"):
                    skipped += 1
                    continue
                if isinstance(item, dict) and item.get("last_checked") == RUN_DATE and item.get("mcap_group") in ("Under500", "Preferred"):
                    skipped += 1
                    continue
                to_do.append(c)
            not_done = [c for c in to_do if c not in self.state]
            done_old = [c for c in to_do if c in self.state]
            all_codes = not_done + done_old
            log("INFO", f"오늘 완료 {skipped}개 제외, 남은 {len(all_codes)}개 (신규 {len(not_done)}개)")
            if not all_codes:
                log("INFO", "오늘 수집할 종목 없음")
                QApplication.quit()
                return
            log("INFO", f"대상 {len(all_codes)}개 수집 시작")
            for idx, code in enumerate(all_codes, start=1):
                if self.stop_requested: break
                try: name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code).strip()
                except: name = ""
                if is_preferred_stock(code, name) or is_derivative_product(name):
                    self.state[code] = {"mcap_group": "Preferred", "name": name, "last_checked": RUN_DATE}
                    continue
                fetch_start = get_start_date_for_code(self.state, code, START_DATE)
                group = self.get_basic_info_and_classify(code)
                if self.stop_requested: break
                if group == "Under500":
                    self.state[code] = {"mcap_group": "Under500", "name": self.current_name or name, "last_checked": RUN_DATE}
                    continue
                if group == "Preferred":
                    self.state[code] = {"mcap_group": "Preferred", "name": name, "last_checked": RUN_DATE}
                    continue
                if group == "Timeout":
                    log("WARN", f"기본정보 실패 스킵: {code}")
                    if CONFIG["AUTO_RESET_ON_ERROR"]:
                        reset_single_code(code)
                    continue
                status = "⏸ 정지중" if self.api_paused else "🚀"
                if idx % 5 == 0 or idx <= 5:
                    log("INFO", f"[{idx}/{len(all_codes)}] {self.current_name}({code}) {self.current_mcap_value}억 [{group}] {status} TR={self.session_tr_count}/{self.total_tr_count} 남은={len(all_codes)-idx}")
                if not self.get_daily(code, fetch_start):
                    log("WARN", f"일봉 실패 {code} - 재시도 위해 초기화")
                    if CONFIG["AUTO_RESET_ON_ERROR"]:
                        reset_single_code(code)
                    continue
                self.get_min(code, 3, fetch_start)
                self.get_min(code, 5, fetch_start)
                self.state[code] = {"last_date": RUN_DATE, "mcap_group": group, "name": self.current_name or name}
                if idx % CONFIG["SAVE_INTERVAL"] == 0:
                    self.flush_checkpoint()
            self.checkpoint_and_finish(f"완료. 세션 {self.session_tr_count}건 / 누적 {self.total_tr_count}건" if not self.stop_requested else f"중단: {self.stop_reason}")
        except Exception as e:
            log("ERROR", f"파이프라인 예외 {e}")
            if CONFIG["AUTO_RESET_ON_ERROR"]:
                log("WARN", "🔄 파이프라인 예외 - 전체 오늘자 자동 초기화")
                auto_reset_today_state()
            self.checkpoint_and_finish(f"예외 종료: {e}")
        finally:
            QApplication.quit()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = KiwoomRateLimited()
    sys.exit(app.exec_())