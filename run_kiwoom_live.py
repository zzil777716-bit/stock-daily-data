import sys
import os
import hashlib
import random
from datetime import datetime
from pathlib import Path
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QTextEdit, QListWidget, QGroupBox, QSplitter, QComboBox, QPushButton
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject
from PyQt5.QAxContainer import QAxWidget

# 코어 엔진 임포트
from core_engine import (
    DatabaseCore, 
    DurableDatabaseWriter, 
    EventMatcherEngine, 
    BarAggregator, 
    IntegrityValidator, 
    generate_session_id, 
    get_current_timestamp
)

# [치명적 결함 #1 해결] 백그라운드 스레드와 메인 스레드 간 안전한 통신용 시그널 브릿지
class WorkerSignals(QObject):
    tick_processed = pyqtSignal(dict)
    log_emitted = pyqtSignal(str)

class KiwoomLiveCollector(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("홍군 퀀트 v7.4.2 - 실전 감사 수집기 & 모니터 (최종 방어 버전)")
        self.resize(900, 650)
        
        # 시그널 브릿지 연결
        self.signals = WorkerSignals()
        self.signals.tick_processed.connect(self._handle_tick_main_thread)
        self.signals.log_emitted.connect(self._append_log_text)
        
        # 1. 코어 엔진 초기화 (Audit Trail 세션 가동)
        db_dir = Path("database")
        db_dir.mkdir(exist_ok=True)
        self.db_path = db_dir / "quant_research_live.db"
        self.session_id = generate_session_id()
        
        self.db_core = DatabaseCore(self.db_path, self.session_id)
        self.writer = DurableDatabaseWriter(self.db_core)
        self.writer.start()
        
        self.matcher = EventMatcherEngine(self.db_core)
        self.aggregator = BarAggregator(self.writer)
        self.validator = IntegrityValidator(self.db_core)
        
        self.captured_stocks = {} # {code: name}
        self.tick_counts = {}     # {code: count}
        self.condition_map = {}   # {name: index}
        self.ui_update_pending = False

        # [치명적 결함 #7 해결] 충돌 방지를 위한 동적 화면번호 생성 (1000~9999)
        self.screen_cond = str(random.randint(1000, 4999))
        self.screen_real = str(random.randint(5000, 8999))

        # 2. UI 구성
        self._init_ui()
        
        # [치명적 결함 #2 해결] 틱마다 UI를 갱신하지 않고 0.5초마다 일괄 갱신하여 프리징 차단
        self.ui_timer = QTimer(self)
        self.ui_timer.setInterval(500)
        self.ui_timer.timeout.connect(self._flush_ui_updates)
        self.ui_timer.start()

        # 3. 키움 OpenAPI OCX 컨트롤 생성 및 연결
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self._init_kiwoom_events()
        
        self.log_message(f"[*] 키움 API 로그인 요청 전송 (세션: {self.session_id})")
        self.kiwoom.dynamicCall("CommConnect()")

    def _init_ui(self):
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # 상단 상태 패널
        top_layout = QHBoxLayout()
        self.lbl_status = QLabel("상태: [로그인 대기중...]", self)
        self.lbl_status.setStyleSheet("font-weight: bold; color: #d9534f; font-size: 14px;")
        self.lbl_session = QLabel(f"세션: {self.session_id}", self)
        self.lbl_session.setStyleSheet("font-size: 12px; color: #555;")
        
        top_layout.addWidget(self.lbl_status)
        top_layout.addStretch()
        top_layout.addWidget(self.lbl_session)
        main_layout.addLayout(top_layout)

        # 조건식 선택 제어 패널
        control_group = QGroupBox("조건식 선택 및 실시간 감시 제어")
        control_layout = QHBoxLayout()
        
        self.combo_conditions = QComboBox()
        self.combo_conditions.addItem("로그인 및 조건식 로드 대기 중...")
        self.combo_conditions.setEnabled(False)
        
        self.btn_start_condition = QPushButton("선택 조건식 실시간 감시 시작")
        self.btn_start_condition.setEnabled(False)
        self.btn_start_condition.setStyleSheet("font-weight: bold; background-color: #5cb85c; color: white; padding: 5px;")
        self.btn_start_condition.clicked.connect(self._on_start_condition_clicked)
        
        control_layout.addWidget(QLabel("사용할 조건식:"))
        control_layout.addWidget(self.combo_conditions, stretch=3)
        control_layout.addWidget(self.btn_start_condition, stretch=1)
        control_group.setLayout(control_layout)
        main_layout.addWidget(control_group)

        # 메인 분할 화면 (좌측: 실시간 포착 종목 / 우측: 시스템 상세 로그)
        splitter = QSplitter(Qt.Horizontal)
        
        # 좌측 박스 (포착 종목 리스트)
        left_group = QGroupBox("실시간 감시 및 포착 종목")
        left_layout = QVBoxLayout()
        self.stock_list_widget = QListWidget()
        left_layout.addWidget(self.stock_list_widget)
        left_group.setLayout(left_layout)
        splitter.addWidget(left_group)

        # 우측 박스 (시스템 로그)
        right_group = QGroupBox("실시간 감사 및 수집 로그")
        right_layout = QVBoxLayout()
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: Consolas; font-size: 11px;")
        right_layout.addWidget(self.log_text)
        right_group.setLayout(right_layout)
        splitter.addWidget(right_group)

        splitter.setSizes([300, 550])
        main_layout.addWidget(splitter)

    def log_message(self, msg):
        self.signals.log_emitted.emit(msg)

    def _append_log_text(self, msg):
        timestamp_msg = f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}"
        print(timestamp_msg)
        if hasattr(self, 'log_text'):
            self.log_text.append(timestamp_msg)

    def _init_kiwoom_events(self):
        self.kiwoom.OnEventConnect.connect(self._on_kiwoom_connect)
        self.kiwoom.OnReceiveConditionVer.connect(self._on_receive_condition_ver)
        self.kiwoom.OnReceiveTrCondition.connect(self._on_receive_tr_condition)
        self.kiwoom.OnReceiveRealCondition.connect(self._on_receive_real_condition)
        self.kiwoom.OnReceiveRealData.connect(self._on_receive_real_data)

    def _on_kiwoom_connect(self, err_code):
        if err_code == 0:
            self.log_message("✅ 키움 API 로그인 성공!")
            self.lbl_status.setText("상태: [로그인 성공 / 조건식 로드 중]")
            self.lbl_status.setStyleSheet("font-weight: bold; color: #f0ad4e; font-size: 14px;")
            self.kiwoom.dynamicCall("GetConditionLoad()")
        else:
            self.log_message(f"❌ 키움 API 로그인 실패 (에러코드: {err_code})")
            self.lbl_status.setText(f"상태: [로그인 실패 (코드: {err_code})]")
            self.lbl_status.setStyleSheet("font-weight: bold; color: #d9534f; font-size: 14px;")

    def _on_receive_condition_ver(self):
        self.log_message("📋 조건식 버전 수신 완료. 조건식 목록을 가져옵니다.")
        raw_list = self.kiwoom.dynamicCall("GetConditionNameList()")
        if raw_list:
            self.combo_conditions.clear()
            self.condition_map.clear()
            
            # EUC-KR 인코딩 깨짐 방어 정제
            try:
                if isinstance(raw_list, str):
                    fixed_list = raw_list.encode('latin1').decode('euc-kr')
                else:
                    fixed_list = raw_list
            except Exception:
                fixed_list = raw_list

            conditions = fixed_list.split(";")
            count = 0
            for cond in conditions:
                if cond and "^" in cond:
                    idx_str, name = cond.split("^", 1)
                    idx = int(idx_str)
                    self.condition_map[name] = idx
                    self.combo_conditions.addItem(name)
                    self.log_message(f"🔍 발견된 조건식: [{idx}] {name}")
                    count += 1
            
            if count > 0:
                self.combo_conditions.setEnabled(True)
                self.btn_start_condition.setEnabled(True)
                self.lbl_status.setText("상태: [조건식 선택 대기 중]")
                self.lbl_status.setStyleSheet("font-weight: bold; color: #337ab7; font-size: 14px;")
                self.log_message(f"✨ 총 {count}개의 조건식이 로드되었습니다. 원하는 조건식을 선택 후 시작 버튼을 누르세요.")

    def _on_start_condition_clicked(self):
        selected_name = self.combo_conditions.currentText()
        if not selected_name or selected_name not in self.condition_map:
            self.log_message("⚠️ 유효한 조건식을 선택해주세요.")
            return
            
        idx = self.condition_map[selected_name]
        
        # [치명적 결함 #4 해소] 선택한 조건식을 명확하게 SendCondition 등록
        ret = self.kiwoom.dynamicCall("SendCondition(QString, QString, int, int)", self.screen_cond, selected_name, idx, 1)
        if ret == 1:
            self.log_message(f"🚀 실시간 조건검색 등록 성공: [{idx}] {selected_name} (화면번호: {self.screen_cond})")
            self.lbl_status.setText(f"상태: [감시 중 - {selected_name}]")
            self.lbl_status.setStyleSheet("font-weight: bold; color: #5cb85c; font-size: 14px;")
            self.combo_conditions.setEnabled(False)
            self.btn_start_condition.setEnabled(False)
        else:
            self.log_message(f"❌ 실시간 조건검색 등록 실패: [{idx}] {selected_name}")

    def _on_receive_tr_condition(self, scr_no, code_list, condition_name, index, next):
        if not code_list:
            return
        codes = [c.strip() for c in code_list.split(";") if c.strip()]
        self.log_message(f"📊 조건식 초기 조회 종목 수신 ({condition_name}): {len(codes)}개")
        for code in codes:
            self._register_stock_watch(code, condition_name)

    def _on_receive_real_condition(self, code, type, condition_name, condition_index):
        if type == "I":
            self.log_message(f"🚨 [실시간 조건 진입] 종목코드: {code} | 조건: {condition_name}")
            
            self.writer.queue.put(("insert_event", {
                "code": code,
                "strategy_id": condition_name,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "signal_received_at": get_current_timestamp(),
                "event_type": "CONDITION_ENTRY",
                "trigger_condition": condition_name
            }, None))
            
            self._register_stock_watch(code, condition_name)
            
        elif type == "D":
            self.log_message(f"📤 [실시간 조건 이탈] 종목코드: {code} | 조건: {condition_name}")
            if code in self.captured_stocks:
                self.kiwoom.dynamicCall("SetRealRemove(QString, QString)", self.screen_real, code)
                del self.captured_stocks[code]
                # [치명적 결함 #3 해결] 메모리 누수 방지를 위해 tick_counts도 함께 삭제
                self.tick_counts.pop(code, None)
                self.ui_update_pending = True

    def _register_stock_watch(self, code, trigger_name):
        if code not in self.captured_stocks:
            name_raw = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code)
            try:
                name = name_raw.encode('latin1').decode('euc-kr').strip() if isinstance(name_raw, str) else code
            except Exception:
                name = name_raw.strip() if name_raw else code
                
            self.captured_stocks[code] = name if name else code
            self.tick_counts[code] = 0
            self.kiwoom.dynamicCall("SetRealReg(QString, QString, QString, QString)", self.screen_real, code, "10;13;14;15;20", "1")
            self.log_message(f"📈 실시간 체결 감시 등록 완료: {self.captured_stocks[code]} ({code})")
            self.ui_update_pending = True

    def _flush_ui_updates(self):
        if self.ui_update_pending:
            self.stock_list_widget.clear()
            for code, name in self.captured_stocks.items():
                cnt = self.tick_counts.get(code, 0)
                self.stock_list_widget.addItem(f"• {name} ({code}) - 수신 틱: {cnt}개")
            self.ui_update_pending = False

    def _on_receive_real_data(self, code, real_type, data):
        if real_type == "주식체결":
            current_str = self.kiwoom.dynamicCall("GetCommRealData(QString, int)", code, 10)
            current = abs(int(current_str)) if current_str and current_str.strip() else 0
            
            accum_vol_str = self.kiwoom.dynamicCall("GetCommRealData(QString, int)", code, 13)
            accum_volume = int(accum_vol_str) if accum_vol_str and accum_vol_str.strip() else 0
            
            accum_val_str = self.kiwoom.dynamicCall("GetCommRealData(QString, int)", code, 14)
            accum_trading_value = float(accum_val_str) if accum_val_str and accum_val_str.strip() else 0.0
            
            # [치명적 결함 #6 해결] 체결량(FID 15) 원시값 유지하여 매수/매도 방향성 보존
            trade_vol_str = self.kiwoom.dynamicCall("GetCommRealData(QString, int)", code, 15)
            trade_volume = int(trade_vol_str) if trade_vol_str and trade_vol_str.strip() else 0
            
            trade_time_str = self.kiwoom.dynamicCall("GetCommRealData(QString, int)", code, 20)
            trade_time = trade_time_str.strip() if trade_time_str and trade_time_str.strip() else datetime.now().strftime("%H%M%S")
            
            if current <= 0:
                return

            received_at = get_current_timestamp()
            session_date = datetime.now().strftime("%Y-%m-%d")
            
            if code in self.tick_counts:
                self.tick_counts[code] += 1
                self.ui_update_pending = True

            payload_raw = f"code={code}|trade_timestamp={trade_time}|price={current}|trade_volume={trade_volume}|accum_volume={accum_volume}|accum_trading_value={accum_trading_value}"
            payload_hash = hashlib.sha256(payload_raw.encode()).hexdigest()

            tick_payload = {
                "code": code, "date": session_date, "trade_timestamp": trade_time,
                "received_at": received_at, "price": current, "trade_volume": trade_volume,
                "accum_volume": accum_volume, "accum_trading_value": accum_trading_value,
                "payload_hash": payload_hash
            }
            
            # [치명적 결함 #1 해결] DB 저장이 끝나면 시그널을 통해 메인 스레드로 전달하여 안전하게 처리
            def handle_tick_persisted(tick_id, seq):
                persisted_tick = dict(tick_payload)
                persisted_tick["tick_id"] = tick_id
                persisted_tick["ingest_sequence"] = seq
                self.signals.tick_processed.emit(persisted_tick)

            self.writer.queue.put(("insert_tick", tick_payload, handle_tick_persisted))

    def _handle_tick_main_thread(self, persisted_tick):
        # 메인 스레드에서 안전하게 실행되는 결정론적 매칭 및 5분봉 집계
        self.matcher.evaluate_and_match(persisted_tick)
        self.aggregator.add_tick(persisted_tick)

    def closeEvent(self, event):
        self.log_message("\n[*] 프로그램 종료 요청 감지. 안전한 종료(Graceful Shutdown) 수행 중...")
        self.writer.queue.put(("expire_pending", None, None))
        self.writer.stop()
        
        self.log_message("\n🔍 장 마감 Integrity Validator 검증 리포트:")
        report = self.validator.run_validation()
        for k, v in report.items():
            self.log_message(f" - {k}: {v}")
            
        self.db_core.close()
        self.log_message("[*] 모든 증거 사슬이 안전하게 저장되었습니다.")
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    collector = KiwoomLiveCollector()
    collector.show()
    sys.exit(app.exec_())