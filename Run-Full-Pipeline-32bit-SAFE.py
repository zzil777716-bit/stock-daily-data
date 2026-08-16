
import sys
import time
import os
import sqlite3
import pandas as pd
from pathlib import Path
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QEventLoop
from PyQt5.QAxContainer import QAxWidget

BASE_DIR = Path(r"C:\Users\HONG\Desktop\구글드라이브(집)\주식자동화\파이프라인")
DB_PATH = BASE_DIR / "quant_research_raw.db"
OUTPUT_DIR = BASE_DIR / "daily_prices_split"
CHECKPOINT_PATH = BASE_DIR / "checkpoint.txt"

START_DATE = "20160101"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class SafeTenYearsPipelineBot(QAxWidget):
    def __init__(self):
        super().__init__()
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.login_event_loop = QEventLoop()
        self.tr_event_loop = QEventLoop()
        self.tr_data = []
        self.stock_info = {}
        self.kospi_set = set()
        self.last_prev_next = "0"
        self.init_signal()
        self.init_db()

    def init_signal(self):
        self.OnEventConnect.connect(self.event_connect)
        self.OnReceiveTrData.connect(self.receive_tr_data)

    def init_db(self):
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                date TEXT, code TEXT, name TEXT, market TEXT,
                open REAL, high REAL, low REAL, close REAL,
                volume INTEGER, amount REAL, market_cap REAL,
                shares INTEGER, as_of_time TEXT,
                PRIMARY KEY (date, code)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collection_log (
                code TEXT PRIMARY KEY,
                name TEXT,
                row_count INTEGER,
                last_updated TEXT
            )
        """)
        conn.commit()
        conn.close()

    def comm_connect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def event_connect(self, err_code):
        if err_code == 0:
            print("[KIWOOM] 로그인 성공")
        else:
            print(f"[KIWOOM] 로그인 실패: {err_code}")
        if self.login_event_loop.isRunning():
            self.login_event_loop.exit()

    def get_code_list_by_market(self, market_type):
        data = self.dynamicCall("GetCodeListByMarket(QString)", str(market_type))
        return data.split(';')[:-1]

    def get_master_stock_name(self, code):
        return self.dynamicCall("GetMasterCodeName(QString)", str(code))

    def fetch_stock_basic_info(self, code):
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10001_req", "opt10001", 0, "0102")
        self.tr_event_loop.exec_()

    def db_day_chart_a_full(self, code):
        all_rows = []
        prev_next = 0
        base_date = ""
        while True:
            self.tr_data = []
            self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
            self.dynamicCall("SetInputValue(QString, QString)", "기준일자", base_date)
            self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
            self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10081_req", "opt10081", prev_next, "0101")
            self.tr_event_loop.exec_()
            if not self.tr_data:
                break
            all_rows.extend(self.tr_data)
            oldest = min([r['date_raw'] for r in self.tr_data])
            if oldest < START_DATE:
                break
            if self.last_prev_next != "2":
                break
            prev_next = 2
            base_date = oldest
            time.sleep(0.35)

        final = [r for r in all_rows if r['date_raw'] >= START_DATE]
        seen = set()
        deduped = []
        for r in final:
            key = (r['date'], r['code'])
            if key not in seen:
                seen.add(key)
                r.pop('date_raw', None)
                deduped.append(r)
        deduped.sort(key=lambda x: x['date'])
        return deduped

    def receive_tr_data(self, screen_no, rqname, trcode, record_name, prev_next):
        self.last_prev_next = prev_next
        if rqname == "opt10001_req":
            shares_str = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, 0, "상장주식").strip()
            try:
                shares = int(abs(float(shares_str))) if shares_str else 0
            except:
                shares = 0
            self.stock_info = {'shares': shares}
            if self.tr_event_loop.isRunning():
                self.tr_event_loop.exit()
        elif rqname == "opt10081_req":
            code = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, 0, "종목코드").strip()
            name = self.get_master_stock_name(code)
            cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
            rows = []
            market = 'KOSPI' if code in self.kospi_set else 'KOSDAQ'
            for i in range(cnt):
                date = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "일자").strip()
                if not date:
                    continue
                try:
                    open_p = float(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "시가").strip())
                    high_p = float(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "고가").strip())
                    low_p = float(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "저가").strip())
                    close_p = float(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "현재가").strip())
                    volume = int(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "거래량").strip())
                    amount = float(abs(int(self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "거래대금").strip())))
                except:
                    continue
                shares = self.stock_info.get('shares', 0)
                rows.append({
                    'date': f"{date[:4]}-{date[4:6]}-{date[6:]}",
                    'date_raw': date,
                    'code': code, 'name': name, 'market': market,
                    'open': abs(open_p), 'high': abs(high_p), 'low': abs(low_p), 'close': abs(close_p),
                    'volume': volume, 'amount': amount,
                    'market_cap': float(abs(close_p))*shares,
                    'shares': shares, 'as_of_time': 'EOD_CLOSE'
                })
            self.tr_data = rows
            if self.tr_event_loop.isRunning():
                self.tr_event_loop.exit()

def save_checkpoint(idx):
    with open(CHECKPOINT_PATH, 'w', encoding='utf-8') as f:
        f.write(str(idx))

def load_checkpoint():
    if CHECKPOINT_PATH.exists():
        try:
            return int(CHECKPOINT_PATH.read_text(encoding='utf-8').strip())
        except:
            return 0
    return 0

def run_safe_pipeline_32bit():
    app = QApplication(sys.argv)
    bot = SafeTenYearsPipelineBot()
    print(f"[경로] BASE_DIR: {BASE_DIR}")
    print(f"[경로] DB: {DB_PATH}")
    print(f"[경로] OUTPUT: {OUTPUT_DIR}")
    print("[BOT] 키움 API 로그인 시도...")
    bot.comm_connect()
    kospi_codes = bot.get_code_list_by_market(0)
    kosdaq_codes = bot.get_code_list_by_market(10)
    bot.kospi_set = set(kospi_codes)
    total_target = kospi_codes + kosdaq_codes
    print(f"[BOT] 총 대상: {len(total_target)}개")
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT code, row_count FROM collection_log")
    log_map = {r[0]: r[1] for r in cursor.fetchall()}
    completed_codes = {code for code, cnt in log_map.items() if cnt >= 2000}
    print(f"[BOT] 이미 완료된 종목: {len(completed_codes)}개")
    start_idx = load_checkpoint()
    if start_idx > 0:
        print(f"[BOT] 체크포인트 발견: {start_idx}번째부터 재시작")
    batch_count = 0
    for idx, code in enumerate(total_target):
        if idx < start_idx:
            continue
        if code in completed_codes:
            continue
        try:
            name = bot.get_master_stock_name(code)
            print(f"[{idx+1}/{len(total_target)}] {name}({code}) 수집 중...")
            bot.fetch_stock_basic_info(code)
            time.sleep(0.3)
            rows = bot.db_day_chart_a_full(code)
            if rows:
                print(f" -> {len(rows)}일 확보, DB 저장")
                for r in rows:
                    cursor.execute("INSERT OR REPLACE INTO daily_prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                   (r['date'], r['code'], r['name'], r['market'], r['open'], r['high'], r['low'], r['close'], r['volume'], r['amount'], r['market_cap'], r['shares'], r['as_of_time']))
                cursor.execute("INSERT OR REPLACE INTO collection_log VALUES (?,?,?,datetime('now','localtime'))",
                               (code, name, len(rows)))
                conn.commit()
            else:
                print(" -> 데이터 없음, 스킵")
                cursor.execute("INSERT OR REPLACE INTO collection_log VALUES (?,?,?,datetime('now','localtime'))",
                               (code, name, 0))
                conn.commit()
            save_checkpoint(idx+1)
            batch_count += 1
            time.sleep(0.5)
            if batch_count >= 700:
                print("\n[BOT] 700개 수집 완료! 60초 휴식...\n")
                for sec in range(60, 0, -1):
                    sys.stdout.write(f"\r[휴식 중] 남은 시간: {sec}초 ")
                    sys.stdout.flush()
                    time.sleep(1)
                print("\n[BOT] 재개\n")
                batch_count = 0
        except Exception as e:
            print(f"[WARNING] {code} 에러: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1)
            continue
    print("[BOT] 전체 종목 수집 완료!")
    print("[BOT] 32비트 안전 분할 저장 시작...")
    cursor.execute("SELECT DISTINCT substr(date,1,4) as y FROM daily_prices ORDER BY y")
    years = [r[0] for r in cursor.fetchall()]
    for y in years:
        print(f" {y}년 추출...")
        df = pd.read_sql(f"SELECT * FROM daily_prices WHERE date LIKE '{y}%' ORDER BY code, date", conn)
        df.to_parquet(str(OUTPUT_DIR / f"{y}.parquet"), compression='snappy', index=False)
        df.to_csv(str(OUTPUT_DIR / f"{y}.csv.gz"), index=False, encoding="utf-8-sig", compression='gzip')
        print(f" -> {y}.parquet / {y}.csv.gz ({len(df)} rows)")
        del df
    conn.close()
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
    print(f"[DONE] 완료! 폴더 확인: {OUTPUT_DIR}")

if __name__ == "__main__":
    run_safe_pipeline_32bit()
