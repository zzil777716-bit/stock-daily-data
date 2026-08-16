# synthetic_replay_test.py
import os
import time
from pathlib import Path
from core_engine import DatabaseCore, DurableDatabaseWriter, EventMatcherEngine, BarAggregator, IntegrityValidator, generate_session_id, get_current_timestamp

def run_synthetic_replay():
    print("🧪 [GATE 1] Synthetic Replay Test 시작...")
    
    db_path = Path("database/synthetic_test.db")
    if db_path.exists():
        db_path.unlink() # 테스트를 위해 기존 DB 삭제 후 깨끗이 시작
        
    session_id = generate_session_id()
    db_core = DatabaseCore(db_path, session_id)
    writer = DurableDatabaseWriter(db_core)
    writer.start()
    
    matcher = EventMatcherEngine(db_core)
    aggregator = BarAggregator(writer)
    validator = IntegrityValidator(db_core)
    
    # 1. 가상 신호 이벤트 주입 (SIGNAL EVENT)
    print("📝 가상 신호 이벤트 등록 중...")
    sig_time = get_current_timestamp()
    
    def on_event_registered(event_id):
        print(f"   -> 신호 등록 완료 [Event ID: {event_id}] (수신시각: {sig_time})")

    writer.queue.put(("insert_event", {
        "code": "005930",
        "strategy_id": "SYNTHETIC_BREAKOUT",
        "date": "2026-08-13",
        "signal_received_at": sig_time,
        "event_type": "CONDITION_ENTRY",
        "trigger_condition": "주도주돌파"
    }, on_event_registered))
    
    # 이벤트가 큐에 반영될 짝짓기 시간 대기
    time.sleep(0.5)

    # 2. 가상 Raw Ticks 주입 (RAWS TICKS)
    print("📈 가상 체결 틱 주입 중 (총 3개)...")
    
    ticks_to_replay = [
        {"price": 70000, "vol": 100, "acc_vol": 100, "acc_val": 7000000.0, "time": "090001"},
        {"price": 70100, "vol": 150, "acc_vol": 250, "acc_val": 17515000.0, "time": "090002"},
        {"price": 70200, "vol": 200, "acc_vol": 450, "acc_val": 31555000.0, "time": "090003"}
    ]

    for t in ticks_to_replay:
        payload_raw = f"005930|{t['time']}|{t['price']}|{t['vol']}|{t['acc_vol']}|{t['acc_val']}"
        import hashlib
        p_hash = hashlib.sha256(payload_raw.encode()).hexdigest()
        
        def handle_tick_persisted(tick_id, seq):
            persisted = {
                "tick_id": tick_id, "ingest_sequence": seq, "code": "005930",
                "date": "2026-08-13", "trade_timestamp": t["time"], "received_at": get_current_timestamp(),
                "price": t["price"], "trade_volume": t["vol"], "accum_volume": t["acc_vol"],
                "accum_trading_value": t["acc_val"]
            }
            # 틱이 DB에 박힌 직후 인과관계 매칭 및 5분봉 집계 수행
            matcher.evaluate_and_match(persisted)
            aggregator.add_tick(persisted)
            print(f"   -> 틱 저장 및 매칭 완료 [Tick ID: {tick_id}, Seq: {seq}, Price: {t['price']}]")

        writer.queue.put(("insert_tick", {
            "code": "005930", "date": "2026-08-13", "trade_timestamp": t["time"],
            "received_at": get_current_timestamp(), "price": t["price"],
            "trade_volume": t["vol"], "accum_volume": t["acc_vol"], "accum_trading_value": t["acc_val"],
            "payload_hash": p_hash
        }, handle_tick_persisted))
        time.sleep(0.2)

    # 3. 바 마감 처리
    aggregator.finalize_and_flush("005930", "09:05")
    
    # 4. 종료 대기 (Guaranteed Delivery Queue Drain)
    writer.stop()
    
    # 5. 무결성 검증 리포트 실행
    print("\n🔍 무결성 검증 리포트(Integrity Validator) 실행 중...")
    report = validator.run_validation()
    
    print("-" * 40)
    for k, v in report.items():
        print(f" - {k}: {v}")
    print("-" * 40)
    
    db_core.close()
    
    if report.get("STATUS") == "PASS":
        print("\n✨ SYNTHETIC REPLAY: PASS 🎉")
        print("-> 축하합니다! 데이터 증거 사슬 무결성 검증을 완벽하게 통과했습니다.")
    else:
        print("\n❌ SYNTHETIC REPLAY: FAIL")
        print("-> 증거 사슬에 결함이 있습니다. 실전 수집을 진행할 수 없습니다.")

if __name__ == "__main__":
    run_synthetic_replay()