#!/usr/bin/env python3
"""
주식 기본정보 수집기 테스트 스크립트
OPT10001을 사용한 종목 기본정보 수집 및 DB 저장 테스트
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.api.base_session import create_kiwoom_session
from src.collectors.stock_info import StockInfoCollector, collect_stock_info_batch
from src.market.code_collector import StockCodeCollector
from src.core.database import get_database_manager


def setup_kiwoom_session():
    """키움 세션 준비"""
    print("🔌 키움 세션 준비")
    print("=" * 40)

    try:
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if session and session.is_ready():
            print("✅ 키움 세션 준비 완료")
            return session
        else:
            print("❌ 키움 세션 준비 실패")
            return None

    except Exception as e:
        print(f"❌ 키움 세션 준비 실패: {e}")
        return None


def test_database_preparation():
    """데이터베이스 준비 테스트"""
    print("\n🗄️ 데이터베이스 준비 테스트")
    print("=" * 40)

    try:
        # 데이터베이스 매니저 생성
        db_manager = get_database_manager()

        # 연결 테스트
        if db_manager.test_connection():
            print("✅ 데이터베이스 연결 성공")
        else:
            print("❌ 데이터베이스 연결 실패")
            return False

        # 테이블 생성
        db_manager.create_tables()
        print("✅ 테이블 생성 완료")

        # 테이블 정보 확인
        table_info = db_manager.get_table_info()
        print("📊 테이블 현황:")
        for table, count in table_info.items():
            print(f"   📋 {table}: {count:,}개")

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 준비 실패: {e}")
        return False


def get_test_stock_codes(session):
    """테스트용 종목코드 수집"""
    print("\n📈 테스트용 종목코드 수집")
    print("=" * 40)

    try:
        connector = session.get_connector()
        code_collector = StockCodeCollector(connector)

        # 코스피 5개 + 코스닥 5개 수집
        print("🔄 코스피 종목코드 수집 중...")
        kospi_codes = code_collector.get_kospi_codes()

        print("🔄 코스닥 종목코드 수집 중...")
        kosdaq_codes = code_collector.get_kosdaq_codes()

        if not kospi_codes or not kosdaq_codes:
            print("❌ 종목코드 수집 실패")
            return []

        # 테스트용: 각각 5개씩
        test_codes = kospi_codes[:5] + kosdaq_codes[:5]

        print(f"✅ 테스트 종목코드 준비 완료: {len(test_codes)}개")
        print("📋 테스트 종목 목록:")
        for i, code in enumerate(test_codes, 1):
            market = "KOSPI" if code in kospi_codes else "KOSDAQ"
            print(f"   {i:2d}. {code} ({market})")

        return test_codes

    except Exception as e:
        print(f"❌ 종목코드 수집 실패: {e}")
        return []


def test_single_stock_collection(session, stock_code):
    """단일 종목 정보 수집 테스트"""
    print(f"\n📊 단일 종목 수집 테스트: {stock_code}")
    print("=" * 40)

    try:
        collector = StockInfoCollector(session)

        # 수집 전 상태 확인
        status_before = collector.get_collection_status()
        print(f"🔍 수집 전 상태: {status_before}")

        # 단일 종목 수집
        print(f"🔄 {stock_code} 정보 수집 중...")
        success, is_new = collector.collect_single_stock_info(stock_code)

        if success:
            print(f"✅ {stock_code} 수집 성공 ({'새 데이터' if is_new else '업데이트'})")

            # 수집된 데이터 확인
            stock_info = collector.db_service.get_stock_info(stock_code)
            if stock_info:
                print(f"📋 수집된 정보:")
                print(f"   종목명: {stock_info.get('name', 'N/A')}")
                print(f"   현재가: {stock_info.get('current_price', 0):,}원")
                print(f"   등락률: {stock_info.get('change_rate', 0) / 100:.2f}%")
                print(f"   거래량: {stock_info.get('volume', 0):,}주")
                print(f"   시가총액: {stock_info.get('market_cap', 0):,}원")
                print(f"   업데이트: {stock_info.get('last_updated', 'N/A')}")

            return True
        else:
            print(f"❌ {stock_code} 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 단일 종목 수집 테스트 실패: {e}")
        return False


def test_batch_collection(session, stock_codes):
    """배치 수집 테스트"""
    print(f"\n🚀 배치 수집 테스트")
    print("=" * 40)

    try:
        print(f"📊 대상 종목: {len(stock_codes)}개")
        print(f"🧪 테스트 모드: 처음 5개만 수집")

        # 배치 수집 실행
        results = collect_stock_info_batch(
            session=session,
            stock_codes=stock_codes,
            test_mode=True  # 5개만 수집
        )

        if 'error' in results:
            print(f"❌ 배치 수집 실패: {results['error']}")
            return False

        # 결과 출력
        print(f"\n📋 배치 수집 결과:")
        print(f"   ✅ 새로 수집: {results['total_collected']}개")
        print(f"   🔄 업데이트: {results['total_updated']}개")
        print(f"   ⏭️ 건너뛰기: {results['total_skipped']}개")
        print(f"   ❌ 실패: {results['total_errors']}개")
        print(f"   ⏱️ 소요시간: {results['elapsed_time']:.1f}초")

        # 성공한 종목들 상세 정보
        if results['success'] or results['updated']:
            print(f"\n📈 수집 성공 종목들:")
            all_success = results['success'] + results['updated']

            from src.core.database import get_database_service
            db_service = get_database_service()

            for code in all_success[:3]:  # 처음 3개만 출력
                info = db_service.get_stock_info(code)
                if info:
                    print(f"   📊 {code}: {info.get('name', 'N/A')} - "
                          f"{info.get('current_price', 0):,}원 "
                          f"({info.get('change_rate', 0) / 100:+.2f}%)")

        return results['total_collected'] + results['total_updated'] > 0

    except Exception as e:
        print(f"❌ 배치 수집 테스트 실패: {e}")
        return False


def test_update_logic(session, stock_codes):
    """업데이트 로직 테스트"""
    print(f"🔄 일일 업데이트 로직 테스트")
    print("=" * 40)

    try:
        collector = StockInfoCollector(session)

        if not stock_codes:
            print("❌ 테스트할 종목코드가 없음")
            return False

        test_code = stock_codes[0]
        print(f"🧪 테스트 종목: {test_code}")

        # 1차 수집 (강제)
        print(f"📥 1차 수집 (강제 모드)...")
        collector.collect_and_update_stocks([test_code], test_mode=False, force_update=True)

        # 업데이트 필요 여부 확인
        print(f"🔍 업데이트 필요 여부 확인...")
        needs_update = collector.is_update_needed(test_code)
        print(f"   오늘 수집 필요: {'예' if needs_update else '아니오'}")

        # 2차 수집 (일반 모드 - 건너뛰어야 함)
        print(f"📥 2차 수집 (일반 모드)...")
        results = collector.collect_and_update_stocks([test_code], test_mode=False, force_update=False)

        if results['total_skipped'] > 0:
            print(f"✅ 일일 체크 로직 정상 작동 (건너뛰기)")
            return True
        elif results['total_updated'] > 0:
            print(f"🔄 데이터 업데이트됨 (5일 이상 경과)")
            return True
        else:
            print(f"❌ 예상치 못한 결과")
            return False

    except Exception as e:
        print(f"❌ 업데이트 로직 테스트 실패: {e}")
        return False


def test_database_queries():
    """데이터베이스 쿼리 테스트 및 HeidiSQL 쿼리 생성"""
    print(f"\n🔍 데이터베이스 쿼리 테스트")
    print("=" * 40)

    try:
        from src.core.database import get_database_service
        db_service = get_database_service()
        db_manager = get_database_manager()

        # 테이블 정보 확인
        table_info = db_manager.get_table_info()
        print(f"📊 테이블 현황:")
        for table, count in table_info.items():
            print(f"   📋 {table}: {count:,}개")

        # HeidiSQL 쿼리 생성
        print(f"\n💻 HeidiSQL 확인 쿼리:")
        print(f"=" * 30)

        print(f"-- 전체 종목 현황")
        print(f"SELECT market, COUNT(*) as count, AVG(current_price) as avg_price")
        print(f"FROM stocks")
        print(f"WHERE current_price > 0")
        print(f"GROUP BY market;")

        print(f"\n-- 최근 업데이트된 종목 (상위 10개)")
        print(f"SELECT code, name, market, current_price, change_rate/100.0 as change_rate_pct, last_updated")
        print(f"FROM stocks")
        print(f"WHERE last_updated IS NOT NULL")
        print(f"ORDER BY last_updated DESC")
        print(f"LIMIT 10;")

        print(f"\n-- 시가총액 상위 종목 (상위 10개)")
        print(f"SELECT code, name, market, current_price, market_cap")
        print(f"FROM stocks")
        print(f"WHERE market_cap > 0")
        print(f"ORDER BY market_cap DESC")
        print(f"LIMIT 10;")

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 쿼리 테스트 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 주식 기본정보 수집기 테스트")
    print("=" * 50)

    # 테스트 목록
    tests = [
        ("데이터베이스 준비", test_database_preparation),
    ]

    results = []
    session = None
    test_codes = []

    # 1단계: 키움 세션 준비
    session = setup_kiwoom_session()
    if not session:
        print("\n❌ 키움 세션 준비 실패로 테스트 중단")
        return False

    results.append(("키움 세션 준비", True))

    # 2단계: 데이터베이스 준비
    db_success = test_database_preparation()
    results.append(("데이터베이스 준비", db_success))

    if not db_success:
        print("\n❌ 데이터베이스 준비 실패로 테스트 중단")
        return False

    # 3단계: 종목코드 수집
    test_codes = get_test_stock_codes(session)
    if test_codes:
        results.append(("종목코드 수집", True))

        # 4단계: 단일 종목 테스트
        single_success = test_single_stock_collection(session, test_codes[0])
        results.append(("단일 종목 수집", single_success))

        # 5단계: 배치 수집 테스트
        batch_success = test_batch_collection(session, test_codes)
        results.append(("배치 수집", batch_success))

        # 6단계: 업데이트 로직 테스트
        update_success = test_update_logic(session, test_codes)
        results.append(("업데이트 로직", update_success))

        # 7단계: 데이터베이스 쿼리 테스트
        query_success = test_database_queries()
        results.append(("데이터베이스 쿼리", query_success))
    else:
        results.append(("종목코드 수집", False))

    # 최종 결과 요약
    print("\n" + "=" * 50)
    print("📋 테스트 결과 요약")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ 성공" if result else "❌ 실패"
        print(f"{test_name:.<25} {status}")
        if result:
            passed += 1

    print(f"\n🎯 전체 결과: {passed}/{total} 테스트 통과")

    if passed == total:
        print("🎉 모든 테스트 통과! 주식정보 수집 시스템 준비 완료.")
        print("💡 다음 단계: 전체 시장 데이터 수집 또는 일봉 데이터 연동")
    elif passed >= total - 2:
        print("✨ 핵심 기능 테스트 통과! 실제 수집 가능.")
        print("💡 일부 실패한 기능들을 점검 후 운영 가능")
    else:
        print("⚠️ 주요 테스트 실패. 키움 API 연결 및 설정을 확인해주세요.")

    return passed >= total - 2


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 사용자가 테스트를 중단했습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 테스트 실행 중 예상치 못한 오류: {e}")
        sys.exit(1)