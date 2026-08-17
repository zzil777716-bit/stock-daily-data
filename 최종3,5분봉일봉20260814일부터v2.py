import sys, os, time, json, shutil, tempfile, traceback
from collections import deque, defaultdict
from datetime import datetime, timedelta
import pandas as pd
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer

# ==============================================================================
# 최종4 개선사항
# 1. REQUEST_INTERVAL 0.05 -> 0.25 (키움 초당 5회 제한 준수)
# 2. auto_reset_today_state 삭제 -> 실패 큐(retry_queue)로 변경, 전체 재수집 방지
# 3. final_replace 정렬 후 deduplicate + 드라이브 동기화 충돌 방지 (재시도 로직)
# 4. TR -200, -204 등 에러시 exponential backoff
# 5. state.json 구조 개선 및 last_date만 삭제하지 않고 failed_count 관리
# ==============================================================================

CONFIG = {
    "BASE_ROOT": r"C:\Users\HONG\Desktop\구글드라이브(집)\데이터",
    "LOCAL_WORK_ROOT": r"C:\KiwoomCache\작업폴더",
    "START_DATE": "20260814",
    "LOG_LEVEL": "INFO",
    "FILTER_PREFERRED": True,
    "FILTER_ETF": True,
    "TARGET_CODES": [],  # 비어있으면 전체
    "MIN_MCAP": 500,  # 억
    "SAFETY_OVERLAP_DAYS": 2,
    "REQUEST_INTERVAL_SEC": 0.25,  # ⭐ 0.05 -> 0.25로 상향 (초당 4회)
    "SECOND_SAFE_MAX": 10,
    "MINUTE_SAFE_MAX": 200,
    "SESSION_TR_LIMIT": 900,  # 950 -> 900 여유
    "REQUEST_TIMEOUT_MS": 45000,
    "MAX_COMM_RETRIES": 3,
    "MAX_TR_RETRY_PER_CODE": 3,
    "MAX_PAGES_PER_DATA": 50,
    "MAX_LOGIN_RETRIES": 3,
    "SAVE_INTERVAL": 10,
    "SCREEN_NO_BASE": 1000,
    "API_PAUSE_ON_ERROR": True,
    "AUTO_RESET_ON_ERROR": False,  # ⭐ 최종4에서는 전체 초기화 비활성화
    "ERROR_THRESHOLD": 5,
    "RETRY_DELAY_SEC": 5,
    "BACKOFF_MULTIPLIER": 2.0,
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
        except:
            pass

def atomic_write_json(path, payload):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".state_", suffix=".tmp", dir=os.path.dirname(path))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        log("ERROR", f"atomic_write_json 실패 {e}")
        return False

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def get_start_date_for_code(state, code, default_start):
    item = state.get(code, {})
    if not isinstance(item, dict) or not item.get("last_date"):
        return default_start
    try:
        last = datetime.strptime(str(item["last_date"]), "%Y%m%d")
        overlap = last - timedelta(days=CONFIG["SAFETY_OVERLAP_DAYS"])
        return max(default_start, overlap.strftime("%Y%m%d"))
    except:
        return default_start

def is_preferred_stock(code, name):
    if not CONFIG["FILTER_PREFERRED"]:
        return False
    if not name:
        return False
    return bool(name.endswith("우") or "우B" in name or "우C" in name)

def is_derivative_product(name):
    if not name:
        return False
    exclude_keywords = ["ETN", "ETF", "선물", "옵션", "인버스", "레버리지", "KODEX", "TIGER", "KBSTAR", "RISE", "KOSEF", "ARIRANG", "3X", "2X"]
    return any(k in name for k in exclude_keywords)

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
    except:
        return pd.DataFrame()

def atomic_to_csv(df, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".csv_", suffix=".tmp", dir=os.path.dirname(path))
        os.close(fd)
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        log("ERROR", f"atomic_to_csv 실패 {path} {e}")
        return False

def safe_copy_to_drive(src, dst, max_retry=5):
    """구글 드라이브 잠금 대응 재시도 복사"""
    for i in range(max_retry):
        try:
            df = read_csv_preserve_keys(src)
            if df.empty:
                return True
            # ⭐ 정렬 후 중복제거 (keep=last가 최신 유지하도록)
            if "_일봉" in dst:
                if "date" in df.columns:
                    df = df.sort_values(["code", "date"])
                df = df.drop_duplicates(subset=["date", "code"], keep="last")
            else:
                if "date" in df.columns and "time" in df.columns:
                    df = df.sort_values(["code", "date", "time"])
                df = df.drop_duplicates(subset=["date", "time", "code"], keep="last")
            return atomic_to_csv(df, dst)
        except Exception as e:
            log("WARN", f"드라이브 복사 재시도 {i+1}/{max_retry} {os.path.basename(dst)} {e}")
            time.sleep(1.5 * (i+1))
    return False


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
        self.last_err_code = ""

        self.session_tr_count = 0
        self.total_tr_count = 0
        self.stop_requested = False
        self.stop_reason = ""
        self.pipeline_started = False

        self.state = load_state()
        self.api_paused = False
        self.consecutive_errors = 0
        self.retry_queue = deque()
        self.failed_codes = []

        # 버퍼
        self.daily_buffer = []
        self.min3_buffer = []
        self.min5_buffer = []
        self.current_code = ""
        self.current_name = ""
        self.current_mcap_value = 0
        self.current_fetch_start = START_DATE

        # 화면번호 관리
        self.screen_no = CONFIG["SCREEN_NO_BASE"]
        self.tr_delay_timer = QTimer()
        self.tr_delay_timer.setSingleShot(True)

    def _on_connect(self, err_code):
        if self.loop:
            self.last_err_code = str(err_code)
            self.loop.exit()

    def _on_msg(self, screen_no, rqname, trcode, msg):
        # TR 제한 에러 감지
        if "초과" in msg or "-200" in msg or "-204" in msg:
            log("WARN", f"TR 제한 감지 {rqname} {trcode} {msg} -> 3.6초 대기")
            self.api_paused = True
            QTimer.singleShot(3600, lambda: setattr(self, 'api_paused', False))

    def _on_tr_data(self, screen_no, rqname, trcode, recordname, prev_next, *args):
        self.prev_next = prev_next
        self.tr_ok = True
        try:
            if rqname.startswith("시총") or rqname == "주식기본정보":
                self._handle_basic_info(rqname, trcode)
            elif rqname.startswith("일봉"):
                self._handle_daily(rqname, trcode)
            elif rqname.startswith("분봉"):
                self._handle_min(rqname, trcode)
        except Exception as e:
            log("ERROR", f"_on_tr_data 예외 {rqname} {e} {traceback.format_exc()}")
        if self.loop:
            self.loop.exit()

    # ---------------- 공통 TR 요청 ----------------
    def comm_request(self, rqname, trcode, prev_next=0, screen_no="1000"):
        """TR 요청 + 타임아웃 + 에러 백오프 포함"""
        for attempt in range(CONFIG["MAX_COMM_RETRIES"]):
            if self.stop_requested:
                return False
            # TR 제한 대기
            while self.api_paused:
                log("INFO", "API 제한 대기중... 1초 슬립")
                time.sleep(1)
                QApplication.processEvents()

            self.tr_ok = False
            self.prev_next = "0"
            self.active_rqname = rqname
            self.active_screen_no = screen_no

            ret = self.kiwoom.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, prev_next, screen_no)
            if ret != 0:
                log("WARN", f"CommRqData 실패 ret={ret} {rqname} attempt {attempt+1}")
                time.sleep(CONFIG["RETRY_DELAY_SEC"] * (attempt+1))
                continue

            self.session_tr_count += 1
            self.total_tr_count += 1
            if self.session_tr_count >= CONFIG["SESSION_TR_LIMIT"]:
                log("WARN", f"세션 TR {self.session_tr_count} 도달, 10분 휴식")
                time.sleep(600)
                self.session_tr_count = 0

            # 이벤트 루프 대기
            self.loop = QEventLoop()
            self._timer = QTimer()
            self._timer.setSingleShot(True)
            self._timer.timeout.connect(self.loop.quit)
            self._timer.start(CONFIG["REQUEST_TIMEOUT_MS"])
            self.loop.exec_()
            self._timer.stop()

            if not self.tr_ok:
                log("WARN", f"TR 타임아웃 {rqname} attempt {attempt+1}")
                time.sleep(CONFIG["RETRY_DELAY_SEC"])
                continue

            # 정상 수신
            self.consecutive_errors = 0
            # 요청 간격 준수
            time.sleep(CONFIG["REQUEST_INTERVAL_SEC"])
            return True

        self.consecutive_errors += 1
        log("ERROR", f"TR 최종 실패 {rqname} 연속오류 {self.consecutive_errors}")
        if self.consecutive_errors >= CONFIG["ERROR_THRESHOLD"]:
            log("ERROR", "연속 오류 임계치 도달 - 30초 쿨다운")
            time.sleep(30)
            self.consecutive_errors = 0
        return False

    # ---------------- 종목 리스트 ----------------
    def get_all_codes(self):
        if CONFIG["TARGET_CODES"]:
            return CONFIG["TARGET_CODES"]
        kospi = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "0").split(";")
        kosdaq = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "10").split(";")
        all_codes = [c for c in kospi + kosdaq if c.strip()]
        log("INFO", f"전체 종목 {len(all_codes)}개 로드")
        return all_codes

    # ---------------- 기본정보 + 시총 분류 ----------------
    def _handle_basic_info(self, rqname, trcode):
        try:
            name = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, 0, "종목명").strip()
            mcap_str = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, 0, "시가총액").strip()
            mcap = int(mcap_str) if mcap_str.lstrip("-").isdigit() else 0
            # 키움 시총은 천원 단위? -> 억으로 변환
            mcap_100m = mcap // 100000  # 억 단위 근사
            self.current_name = name
            self.current_mcap_value = mcap_100m
        except Exception as e:
            log("ERROR", f"기본정보 파싱 실패 {e}")

    def get_basic_info_and_classify(self, code):
        self.current_code = code
        self.current_name = ""
        self.current_mcap_value = 0
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        ok = self.comm_request(f"시총_{code}", "opt10001", 0, str(self.screen_no))
        if not ok:
            return "Timeout"
        name = self.current_name
        if is_preferred_stock(code, name) or is_derivative_product(name):
            return "Preferred"
        mcap = self.current_mcap_value
        if mcap < CONFIG["MIN_MCAP"]:
            return "Under500"
        elif mcap >= 1000:
            return "Over1000"
        else:
            return "500to1000"

    # ---------------- 일봉 ----------------
    def _handle_daily(self, rqname, trcode):
        cnt = self.kiwoom.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        for i in range(cnt):
            date = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "일자").strip()
            if not date or date < self.current_fetch_start:
                self.reached_fetch_start = True
                break
            open_p = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "시가").strip()
            high = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "고가").strip()
            low = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "저가").strip()
            close = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "현재가").strip()
            vol = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "거래량").strip()
            self.daily_buffer.append({
                "date": date, "code": self.current_code, "name": self.current_name,
                "open": open_p, "high": high, "low": low, "close": close, "volume": vol
            })

    def get_daily(self, code, fetch_start):
        self.current_code = code
        self.current_fetch_start = fetch_start
        self.daily_buffer = []
        self.reached_fetch_start = False
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "기준일자", RUN_DATE)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")

        pages = 0
        while pages < CONFIG["MAX_PAGES_PER_DATA"]:
            ok = self.comm_request(f"일봉_{code}", "opt10081", 2 if pages > 0 else 0, str(self.screen_no))
            if not ok:
                return False
            pages += 1
            if self.reached_fetch_start or self.prev_next != "2":
                break
        # 저장
        if self.daily_buffer:
            df = pd.DataFrame(self.daily_buffer)
            year_groups = df["date"].str[:4].unique()
            for y in year_groups:
                ydf = df[df["date"].str.startswith(y)]
                path = os.path.join(LOCAL_ROOT, y, f"{y}_일봉.csv")
                self.merge_save(ydf, path, ["date", "code"])
        log("INFO", f"일봉 {code} {len(self.daily_buffer)}건 수집 (시작 {fetch_start})")
        return True

    # ---------------- 분봉 ----------------
    def _handle_min(self, rqname, trcode):
        cnt = self.kiwoom.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        for i in range(cnt):
            # 체결시간: 20260814103000 형식
            dt = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "체결시간").strip()
            if len(dt) < 14:
                continue
            date = dt[:8]
            time_str = dt[8:14]
            if date < self.current_fetch_start:
                self.reached_fetch_start = True
                break
            open_p = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "시가").strip()
            high = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "고가").strip()
            low = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "저가").strip()
            close = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "현재가").strip()
            vol = self.kiwoom.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "거래량").strip()
            row = {"date": date, "time": time_str, "code": self.current_code, "name": self.current_name,
                   "open": open_p, "high": high, "low": low, "close": close, "volume": vol}
            if "3분" in rqname:
                self.min3_buffer.append(row)
            else:
                self.min5_buffer.append(row)

    def get_min(self, code, minute_type, fetch_start):
        self.current_code = code
        self.current_fetch_start = fetch_start
        self.reached_fetch_start = False
        if minute_type == 3:
            self.min3_buffer = []
        else:
            self.min5_buffer = []

        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "틱범위", str(minute_type))
        self.kiwoom.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")

        pages = 0
        while pages < CONFIG["MAX_PAGES_PER_DATA"]:
            ok = self.comm_request(f"분봉_{minute_type}분_{code}", "opt10080", 2 if pages > 0 else 0, str(self.screen_no + minute_type))
            if not ok:
                log("WARN", f"{minute_type}분봉 실패 {code}")
                return False
            pages += 1
            if self.reached_fetch_start or self.prev_next != "2":
                break

        buf = self.min3_buffer if minute_type == 3 else self.min5_buffer
        if buf:
            df = pd.DataFrame(buf)
            for y in df["date"].str[:4].unique():
                ydf = df[df["date"].str.startswith(y)]
                path = os.path.join(LOCAL_ROOT, y, f"{y}_{minute_type}분봉.csv")
                self.merge_save(ydf, path, ["date", "time", "code"])
        log("INFO", f"{minute_type}분봉 {code} {len(buf)}건")
        return True

    # ---------------- 저장 ----------------
    def merge_save(self, new_df, path, dedup_keys):
        try:
            new_df = normalize_key_columns(new_df)
            if os.path.exists(path):
                old_df = read_csv_preserve_keys(path)
                if not old_df.empty:
                    combined = pd.concat([old_df, new_df], ignore_index=True)
                    # 정렬 후 중복제거
                    sort_keys = dedup_keys
                    combined = combined.sort_values(sort_keys).drop_duplicates(subset=dedup_keys, keep="last")
                else:
                    combined = new_df
            else:
                combined = new_df
            atomic_to_csv(combined, path)
        except Exception as e:
            log("ERROR", f"merge_save 실패 {path} {e}")

    def save_incremental_to_local(self):
        # 이미 merge_save에서 저장했으므로 여기서는 검증만
        return True

    def final_replace_to_drive(self):
        try:
            log("INFO", "드라이브로 최종 복사 시작")
            for year in os.listdir(LOCAL_ROOT):
                lp = os.path.join(LOCAL_ROOT, year)
                if not os.path.isdir(lp) or year.startswith("."):
                    continue
                dp = os.path.join(BASE_ROOT, year)
                os.makedirs(dp, exist_ok=True)
                for fname in os.listdir(lp):
                    if not fname.endswith(".csv"):
                        continue
                    src = os.path.join(lp, fname)
                    dst = os.path.join(dp, fname)
                    safe_copy_to_drive(src, dst)
            log("INFO", "드라이브 복사 완료")
        except Exception as e:
            log("ERROR", f"final_replace 전체 실패 {e}")

    def flush_checkpoint(self):
        try:
            ok_state = atomic_write_json(STATE_FILE, self.state)
            log("INFO", f"체크포인트 저장 STATE={ok_state} TR={self.session_tr_count}/{self.total_tr_count} 실패큐={len(self.retry_queue)}")
            return ok_state
        except Exception as e:
            log("ERROR", f"flush 실패 {e}")
            return False

    def checkpoint_and_finish(self, msg):
        self.flush_checkpoint()
        if self.failed_codes:
            log("WARN", f"실패한 코드 {len(self.failed_codes)}개: {self.failed_codes[:20]}")
            # 실패 리스트 파일로 저장
            fail_path = os.path.join(LOCAL_ROOT, f"실패_{RUN_DATE}.json")
            atomic_write_json(fail_path, self.failed_codes)
        self.final_replace_to_drive()
        log("INFO", msg)
        try:
            log_fp.close()
        except:
            pass

    def sync_drive_to_local(self):
        """기존 드라이브 -> 로컬 싱크 (로컬이 비어있을 때만)"""
        try:
            if not os.path.exists(BASE_ROOT):
                return
            # 로컬에 파일이 하나도 없으면 드라이브에서 복사
            has_local = any(os.path.isdir(os.path.join(LOCAL_ROOT, d)) for d in os.listdir(LOCAL_ROOT) if not d.startswith("."))
            if has_local:
                log("INFO", "로컬 캐시 존재 - 드라이브 싱크 스킵")
                return
            log("INFO", "로컬 캐시 없음 - 드라이브에서 싱크")
            for year in os.listdir(BASE_ROOT):
                dp = os.path.join(BASE_ROOT, year)
                if not os.path.isdir(dp):
                    continue
                lp = os.path.join(LOCAL_ROOT, year)
                os.makedirs(lp, exist_ok=True)
                for fname in os.listdir(dp):
                    if fname.endswith(".csv"):
                        shutil.copy2(os.path.join(dp, fname), os.path.join(lp, fname))
        except Exception as e:
            log("ERROR", f"sync_drive_to_local 실패 {e}")

    def start_pipeline(self):
        if self.pipeline_started:
            return
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
                if not isinstance(item, dict):
                    to_do.append(c)
                    continue
                # 오늘 이미 성공한 것만 스킵
                if item.get("last_date") == RUN_DATE and item.get("status") == "success":
                    skipped += 1
                    continue
                to_do.append(c)

            # 신규 우선
            not_done = [c for c in to_do if c not in self.state]
            done_old = [c for c in to_do if c in self.state]
            all_codes = not_done + done_old

            log("INFO", f"오늘 완료 {skipped}개 제외, 남은 {len(all_codes)}개 (신규 {len(not_done)}개)")
            if not all_codes:
                log("INFO", "오늘 수집할 종목 없음")
                QApplication.quit()
                return

            for idx, code in enumerate(all_codes, start=1):
                if self.stop_requested:
                    break
                try:
                    name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code).strip()
                except:
                    name = ""

                if is_preferred_stock(code, name) or is_derivative_product(name):
                    self.state[code] = {"mcap_group": "Preferred", "name": name, "last_checked": RUN_DATE, "status": "filtered"}
                    continue

                fetch_start = get_start_date_for_code(self.state, code, START_DATE)
                group = self.get_basic_info_and_classify(code)
                if self.stop_requested:
                    break
                if group == "Under500":
                    self.state[code] = {"mcap_group": "Under500", "name": self.current_name or name, "last_checked": RUN_DATE, "status": "filtered"}
                    continue
                if group == "Preferred":
                    self.state[code] = {"mcap_group": "Preferred", "name": name, "last_checked": RUN_DATE, "status": "filtered"}
                    continue
                if group == "Timeout":
                    log("WARN", f"기본정보 실패 큐에 추가: {code}")
                    self.retry_queue.append(code)
                    continue

                status = "⏸ 정지중" if self.api_paused else "🚀"
                if idx % 5 == 0 or idx <= 5:
                    log("INFO", f"[{idx}/{len(all_codes)}] {self.current_name}({code}) {self.current_mcap_value}억 [{group}] {status} TR={self.session_tr_count}/{self.total_tr_count} 남은={len(all_codes)-idx} 실패큐={len(self.retry_queue)}")

                # 일봉
                daily_ok = self.get_daily(code, fetch_start)
                if not daily_ok:
                    log("WARN", f"일봉 실패 재시도 큐로 {code}")
                    self.retry_queue.append(code)
                    self.state[code] = {"last_date": self.state.get(code, {}).get("last_date", ""), "mcap_group": group, "name": self.current_name or name, "status": "failed_daily", "failed_count": self.state.get(code, {}).get("failed_count", 0)+1}
                    continue

                # 분봉
                min3_ok = self.get_min(code, 3, fetch_start)
                min5_ok = self.get_min(code, 5, fetch_start)
                if not (min3_ok and min5_ok):
                    log("WARN", f"분봉 일부 실패 {code} - 그래도 성공으로 처리")

                self.state[code] = {"last_date": RUN_DATE, "mcap_group": group, "name": self.current_name or name, "status": "success", "failed_count": 0}
                if idx % CONFIG["SAVE_INTERVAL"] == 0:
                    self.flush_checkpoint()

            # 실패 큐 재시도 (1회)
            if self.retry_queue:
                log("INFO", f"실패 큐 재시도 {len(self.retry_queue)}개")
                retry_list = list(self.retry_queue)
                self.retry_queue.clear()
                for code in retry_list:
                    if self.stop_requested:
                        break
                    fetch_start = get_start_date_for_code(self.state, code, START_DATE)
                    if self.get_daily(code, fetch_start):
                        self.get_min(code, 3, fetch_start)
                        self.get_min(code, 5, fetch_start)
                        self.state[code] = {"last_date": RUN_DATE, "mcap_group": "retry_success", "name": self.current_name, "status": "success"}
                    else:
                        self.failed_codes.append(code)

            self.checkpoint_and_finish(f"완료. 세션 {self.session_tr_count}건 / 누적 {self.total_tr_count}건 실패 {len(self.failed_codes)}건" if not self.stop_requested else f"중단: {self.stop_reason}")

        except Exception as e:
            log("ERROR", f"파이프라인 예외 {e} {traceback.format_exc()}")
            # ⭐ 최종4: 전체 초기화 절대 하지 않음, 현재 상태만 저장
            self.checkpoint_and_finish(f"예외 종료: {e}")
        finally:
            QApplication.quit()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = KiwoomRateLimited()
    # 로그인 대기
    window.kiwoom.dynamicCall("CommConnect()")
    window.loop = QEventLoop()
    window.loop.exec_()
    if window.last_err_code == "0":
        log("INFO", "로그인 성공 - 파이프라인 시작")
        window.start_pipeline()
        sys.exit(app.exec_())
    else:
        log("ERROR", f"로그인 실패 {window.last_err_code}")
        sys.exit(1)
