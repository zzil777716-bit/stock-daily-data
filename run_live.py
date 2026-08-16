import sys
import os
import sqlite3
import queue
import threading
from datetime import datetime
from pathlib import Path
import hashlib
import json

COLLECTOR_VERSION = "v7.4.2-CORE"

def get_current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def generate_session_id():
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"SESSION_{now_str}"


# -------------------------------------------------------------------------
# 1. Database Core & Durable Writer (SQLite Triggers for Immutability)
# -------------------------------------------------------------------------
class DatabaseCore:
    def __init__(self, db_path, session_id):
        self.db_path = db_path
        self.session_id = session_id
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema_and_triggers()

    def _init_schema_and_triggers(self):
        cursor = self.conn.cursor()
        
        # 1. 세션 관리 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collector_sessions (
                session_id TEXT PRIMARY KEY,
                collector_version TEXT NOT NULL,
                started_at TEXT NOT NULL,
                status TEXT NOT NULL
            )
        """)
        
        # 2. Immutable Raw Ticks (UPDATE/DELETE 원천 차단 Trigger 적용)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                tick_id INTEGER PRIMARY KEY AUTOINCREMENT,
                collector_session_id TEXT NOT NULL,
                ingest_sequence INTEGER NOT NULL,
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                trade_timestamp TEXT NOT NULL,
                received_at TEXT NOT NULL,
                price INTEGER NOT NULL,
                trade_volume INTEGER NOT NULL,
                accum_volume INTEGER NOT NULL,
                accum_trading_value REAL NOT NULL,
                payload_hash TEXT NOT NULL,
                collector_version TEXT NOT NULL,
                data_quality_status TEXT NOT NULL,
                UNIQUE (collector_session_id, ingest_sequence)
            )
        """)
        
        # SQLite Trigger: Ticks 테이블의 UPDATE/DELETE 강제 차단 (Immutable)
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_ticks_immutable_update
            BEFORE UPDATE ON ticks
            BEGIN
                SELECT RAISE(ABORT, 'Audit Violation: ticks table is IMMUTABLE (UPDATE blocked)');
            END;
        """)
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_ticks_immutable_delete
            BEFORE DELETE ON ticks
            BEGIN
                SELECT RAISE(ABORT, 'Audit Violation: ticks table is IMMUTABLE (DELETE blocked)');
            END;
        """)

        # 3. Signal Events
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                collector_session_id TEXT NOT NULL,
                event_ingest_sequence INTEGER NOT NULL,
                code TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                date TEXT NOT NULL,
                signal_received_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                trigger_condition TEXT NOT NULL,
                capture_tick_id INTEGER,
                capture_timestamp TEXT,
                capture_price INTEGER,
                capture_status TEXT NOT NULL,
                collector_version TEXT NOT NULL,
                data_quality_status TEXT NOT NULL,
                FOREIGN KEY(capture_tick_id) REFERENCES ticks(tick_id),
                UNIQUE (collector_session_id, event_ingest_sequence)
            )
        """)

        # 4. Event Match Candidates (감사 로그: 후보군 전체 기록)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_match_candidates (
                candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                tick_id INTEGER NOT NULL,
                ingest_sequence INTEGER NOT NULL,
                received_at TEXT NOT NULL,
                decision TEXT NOT NULL,
                rejection_reason TEXT,
                FOREIGN KEY(event_id) REFERENCES signal_events(event_id),
                FOREIGN KEY(tick_id) REFERENCES ticks(tick_id)
            )
        """)

        # 5. Derived Intraday Bars (Immutable Snapshot)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS intraday_bars (
                bar_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                aggregation_version TEXT NOT NULL,
                open INTEGER NOT NULL,
                high INTEGER NOT NULL,
                low INTEGER NOT NULL,
                close INTEGER NOT NULL,
                volume INTEGER NOT NULL,
                first_tick_id INTEGER NOT NULL,
                last_tick_id INTEGER NOT NULL,
                first_ingest_sequence INTEGER NOT NULL,
                last_ingest_sequence INTEGER NOT NULL,
                tick_count INTEGER NOT NULL,
                bar_trading_value_fid14 REAL NOT NULL,
                bar_trading_value_tick_sum REAL NOT NULL,
                crosscheck_diff_ratio REAL NOT NULL,
                derived_at TEXT NOT NULL,
                data_quality_status TEXT NOT NULL,
                UNIQUE(code, date, time, aggregation_version)
            )
        """)

        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=FULL;") # 최고 수준의 내구성 보장
        
        # 세션 시작 기록
        cursor.execute("""
            INSERT OR IGNORE INTO collector_sessions (session_id, collector_version, started_at, status)
            VALUES (?, ?, ?, 'RUNNING')
        """, (self.session_id, COLLECTOR_VERSION, get_current_timestamp()))
        
        self.conn.commit()

    def close(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE collector_sessions SET status = 'CLOSED' WHERE session_id = ?", (self.session_id,))
            self.conn.commit()
            self.conn.close()
        except:
            pass


class DurableDatabaseWriter(threading.Thread):
    def __init__(self, db_core):
        super().__init__()
        self.db_core = db_core
        self.queue = queue.Queue()
        self.running = True
        self.seq_counter = 1
        self.event_seq_counter = 1

    def run(self):
        cursor = self.db_core.conn.cursor()
        while self.running or not self.queue.empty():
            try:
                task = self.queue.get(timeout=0.2)
                if task is None:
                    self.queue.task_done()
                    break
                
                q_type, data, callback = task
                try:
                    if q_type == "insert_tick":
                        cursor.execute("""
                            INSERT INTO ticks (
                                collector_session_id, ingest_sequence, code, date, trade_timestamp,
                                received_at, price, trade_volume, accum_volume, accum_trading_value,
                                payload_hash, collector_version, data_quality_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            self.db_core.session_id, self.seq_counter, data["code"], data["date"],
                            data["trade_timestamp"], data["received_at"], data["price"],
                            data["trade_volume"], data["accum_volume"], data["accum_trading_value"],
                            data["payload_hash"], COLLECTOR_VERSION, data.get("data_quality_status", "VALID")
                        ))
                        tick_id = cursor.lastrowid
                        seq = self.seq_counter
                        self.seq_counter += 1
                        self.db_core.conn.commit()
                        if callback: callback(tick_id, seq)

                    elif q_type == "insert_event":
                        cursor.execute("""
                            INSERT INTO signal_events (
                                collector_session_id, event_ingest_sequence, code, strategy_id, date,
                                signal_received_at, event_type, trigger_condition, capture_status,
                                collector_version, data_quality_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
                        """, (
                            self.db_core.session_id, self.event_seq_counter, data["code"], data["strategy_id"],
                            data["date"], data["signal_received_at"], data["event_type"],
                            data["trigger_condition"], COLLECTOR_VERSION, "VALID"
                        ))
                        event_id = cursor.lastrowid
                        self.event_seq_counter += 1
                        self.db_core.conn.commit()
                        if callback: callback(event_id)

                    elif q_type == "update_event_match":
                        cursor.execute("""
                            UPDATE signal_events 
                            SET capture_tick_id = ?, capture_timestamp = ?, capture_price = ?, capture_status = 'CONFIRMED'
                            WHERE event_id = ?
                        """, (data["capture_tick_id"], data["capture_timestamp"], data["capture_price"], data["event_id"]))
                        
                        # 후보군 및 매칭 로그 저장
                        for cand in data["candidates"]:
                            cursor.execute("""
                                INSERT INTO event_match_candidates (
                                    event_id, tick_id, ingest_sequence, received_at, decision, rejection_reason
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            """, (data["event_id"], cand["tick_id"], cand["ingest_sequence"], cand["received_at"], cand["decision"], cand["reason"]))
                        self.db_core.conn.commit()

                    elif q_type == "insert_bar":
                        cursor.execute("""
                            INSERT OR IGNORE INTO intraday_bars (
                                code, date, time, aggregation_version, open, high, low, close, volume,
                                first_tick_id, last_tick_id, first_ingest_sequence, last_ingest_sequence,
                                tick_count, bar_trading_value_fid14, bar_trading_value_tick_sum,
                                crosscheck_diff_ratio, derived_at, data_quality_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            data["code"], data["date"], data["time"], data["aggregation_version"],
                            data["open"], data["high"], data["low"], data["close"], data["volume"],
                            data["first_tick_id"], data["last_tick_id"], data["first_ingest_seq"], data["last_ingest_seq"],
                            data["tick_count"], data["bar_trading_value_fid14"], data["bar_trading_value_tick_sum"],
                            data["crosscheck_diff_ratio"], data["derived_at"], data["data_quality_status"]
                        ))
                        self.db_core.conn.commit()

                    elif q_type == "expire_pending":
                        cursor.execute("""
                            UPDATE signal_events SET capture_status = 'EXPIRED' WHERE capture_status = 'PENDING'
                        """)
                        self.db_core.conn.commit()

                except Exception as e:
                    print(f"DB 쓰기 오류 ({q_type}): {e}")
                finally:
                    self.queue.task_done()
            except queue.Empty:
                continue

    def stop(self):
        self.running = False
        self.queue.join()


# -------------------------------------------------------------------------
# 2. Event Matcher Engine (Deterministic & Explicit)
# -------------------------------------------------------------------------
class EventMatcherEngine:
    def __init__(self, db_core):
        self.db_core = db_core

    def evaluate_and_match(self, persisted_tick):
        """
        틱이 DB에 확실히 박힌 후 호출됨.
        received_at >= signal_received_at 조건을 만족하는 PENDING 이벤트 중
        ingest_sequence가 가장 빠른 첫 번째 유효 틱을 확정함.
        """
        cursor = self.db_core.conn.cursor()
        cursor.execute("""
            SELECT event_id, signal_received_at FROM signal_events 
            WHERE capture_status = 'PENDING' AND code = ?
        """, (persisted_tick["code"],))
        pending_events = cursor.fetchall()

        if not pending_events:
            return

        for ev in pending_events:
            ev_id, sig_recv_at = ev
            candidates = cursor.execute("""
                SELECT tick_id, ingest_sequence, received_at FROM ticks 
                WHERE code = ? AND received_at >= ? 
                ORDER BY ingest_sequence ASC
            """, (persisted_tick["code"], sig_recv_at)).fetchall()

            if not candidates:
                continue

            # 결정론적 매칭: 첫 번째 후보가 현재 틱과 일치하는지 확인
            selected = candidates[0]
            cand_records = []
            matched = False

            for c in candidates:
                if c["tick_id"] == persisted_tick["tick_id"] and not matched:
                    cand_records.append({
                        "tick_id": c["tick_id"], "ingest_sequence": c["ingest_sequence"],
                        "received_at": c["received_at"], "decision": "SELECTED", "reason": "First eligible tick after signal reception"
                    })
                    matched = True
                else:
                    cand_records.append({
                        "tick_id": c["tick_id"], "ingest_sequence": c["ingest_sequence"],
                        "received_at": c["received_at"], "decision": "REJECTED", "reason": "Skipped due to prior selection or rule boundary"
                    })

            if matched and selected["tick_id"] == persisted_tick["tick_id"]:
                cursor.execute("""
                    UPDATE signal_events 
                    SET capture_tick_id = ?, capture_timestamp = ?, capture_price = ?, capture_status = 'CONFIRMED'
                    WHERE event_id = ?
                """, (persisted_tick["tick_id"], persisted_tick["received_at"], persisted_tick["price"], ev_id))
                
                for cand in cand_records:
                    cursor.execute("""
                        INSERT INTO event_match_candidates (event_id, tick_id, ingest_sequence, received_at, decision, rejection_reason)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (ev_id, cand["tick_id"], cand["ingest_sequence"], cand["received_at"], cand["decision"], cand["reason"]))
                self.db_core.conn.commit()
                break


# -------------------------------------------------------------------------
# 3. Bar Aggregator (Reconstructable & Versioned)
# -------------------------------------------------------------------------
class BarAggregator:
    def __init__(self, db_writer):
        self.db_writer = db_writer
        self.current_bars = {} # {(code, bucket_time): bar_dict}

    def add_tick(self, tick):
        code = tick["code"]
        trade_time = tick["trade_timestamp"]
        
        if len(trade_time) >= 6:
            hh, mm = int(trade_time[:2]), int(trade_time[2:4])
        else:
            hh, mm = datetime.now().hour, datetime.now().minute
            
        bucket_mm = (mm // 5) * 5
        bucket_time = f"{hh:02d}:{bucket_mm:02d}"
        key = (code, bucket_time)

        if key not in self.current_bars:
            self.current_bars[key] = {
                "code": code, "date": tick["date"], "time": bucket_time,
                "aggregation_version": "BAR_V1",
                "open": tick["price"], "high": tick["price"], "low": tick["price"], "close": tick["price"],
                "volume": tick["trade_volume"],
                "first_tick_id": tick["tick_id"], "last_tick_id": tick["tick_id"],
                "first_ingest_seq": tick["ingest_sequence"], "last_ingest_seq": tick["ingest_sequence"],
                "tick_count": 1,
                "bar_trading_value_fid14": 0.0, # 실시간 누적 차분 연동부
                "bar_trading_value_tick_sum": float(tick["trade_volume"] * tick["price"]),
                "crosscheck_diff_ratio": 0.0,
                "derived_at": get_current_timestamp(),
                "data_quality_status": "VALID"
            }
        else:
            b = self.current_bars[key]
            b["high"] = max(b["high"], tick["price"])
            b["low"] = min(b["low"], tick["price"])
            b["close"] = tick["price"]
            b["volume"] += tick["trade_volume"]
            b["last_tick_id"] = tick["tick_id"]
            b["last_ingest_seq"] = tick["ingest_sequence"]
            b["tick_count"] += 1
            b["bar_trading_value_tick_sum"] += float(tick["trade_volume"] * tick["price"])

    def finalize_and_flush(self, code, current_bucket_time):
        keys_to_flush = [k for k in self.current_bars.keys() if k[0] == code and k[1] != current_bucket_time]
        for key in keys_to_flush:
            bar_data = self.current_bars.pop(key)
            self.db_writer.queue.put(("insert_bar", bar_data, None))


# -------------------------------------------------------------------------
# 4. Integrity Validator (Gate Validator)
# -------------------------------------------------------------------------
class IntegrityValidator:
    def __init__(self, db_core):
        self.db_core = db_core

    def run_validation(self):
        cursor = self.db_core.conn.cursor()
        report = {}

        # 1. Sequence Gap 체크
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM (
                SELECT ingest_sequence - LAG(ingest_sequence, 1, ingest_sequence) OVER (ORDER BY ingest_sequence) as diff 
                FROM ticks WHERE collector_session_id = ?
            ) WHERE diff > 1
        """, (self.db_core.session_id,))
        report["sequence_gaps"] = cursor.fetchone()["cnt"]

        # 2. PENDING 잔존 체크
        cursor.execute("SELECT COUNT(*) as cnt FROM signal_events WHERE capture_status = 'PENDING'")
        report["pending_events"] = cursor.fetchone()["cnt"]

        # 3. Bar Reconstruction 검증 (first ~ last 틱 카운트 대조)
        cursor.execute("""
            SELECT b.bar_id, b.code, b.time, b.tick_count, 
                   (SELECT COUNT(*) FROM ticks t WHERE t.code = b.code AND t.tick_id BETWEEN b.first_tick_id AND b.last_tick_id) as actual_count
            FROM intraday_bars b
        """)
        mismatches = 0
        for row in cursor.fetchall():
            if row["tick_count"] != row["actual_count"]:
                mismatches += 1
        report["bar_reconstruction_mismatches"] = mismatches

        # 종합 상태 판정
        if sum(report.values()) == 0:
            report["STATUS"] = "PASS"
        else:
            report["STATUS"] = "FAIL"

        return report