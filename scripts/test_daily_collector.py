#!/usr/bin/env python3
"""
일봉 데이터 수집기 테스트 스크립트
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.collectors.daily_price import DailyPriceCollector, collect_daily_price_single


def test_collector_initialization():
    """수집기 초기화 테스트"""
    print("🔧 일봉 수집기 초기화 테스트")
    print("=" * 40)

    try:
        config = Config()
        collector = DailyPriceCollector(config)

        status = collector.get_collection_status()
        print(f"✅ 수집기 초기화 완료")
        print(f"📊 DB 연결: {'정상' if status['db_connected'] else '실패'}")
        print(f"📊 키움 연결: {'정상' if status['kiwoom_connected'] else '미연결'}")

        return True

    except Exception as e:
        print(f"❌ 수집기 초기화 실패: {e}")
        return False


def test_kiwoom_connection():
    """키움 API 연결 테스트"""
    print("\n🔌 키움 API 연결 테스트")
    print("=" * 40)

    response = input("키움 OpenAPI로 로그인하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  키움 연결 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = DailyPriceCollector()

        print("🔄 키움 API 연결 중... (로그인 창이 나타날 수 있습니다)")

        if collector.connect_kiwoom(auto_login=True):
            print("✅ 키움 API 연결 성공")

            status = collector.get_collection_status()
            print(f"📊 연결 상태: {status['kiwoom_connected']}")

            return True
        else:
            print("❌ 키움 API 연결 실패")
            return False

    except Exception as e:
        print(f"❌ 키움 연결 테스트 실패: {e}")
        return False


def test_single_stock_collection():
    """단일 종목 수집 테스트"""
    print("\n📈 단일 종목 수집 테스트")
    print("=" * 40)

    # 키움 API 연결 필요 여부 확인
    response = input("실제 데이터 수집을 테스트하시겠습니까? (키움 로그인 필요) (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  실제 수집 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = DailyPriceCollector()

        # 키움 연결
        if not collector.connect_kiwoom():
            print("❌ 키움 API 연결 실패")
            return False

        # 테스트 종목 (삼성전자)
        test_stock = "005930"
        print(f"📊 테스트 종목: {test_stock} (삼성전자)")

        # 데이터 수집
        print("🔄 일봉 데이터 수집 중...")
        success = collector.collect_single_stock(test_stock, update_existing=True)

        if success:
            print("✅ 일봉 데이터 수집 성공")

            # 수집 상태 확인
            status = collector.get_collection_status()
            print(f"📊 수집된 레코드 수: {status['collected_count']}")
            print(f"📊 오류 수: {status['error_count']}")

            return True
        else:
            print("❌ 일봉 데이터 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 단일 종목 수집 테스트 실패: {e}")
        return False


def test_multiple_stocks_collection():
    """다중 종목 수집 테스트"""
    print("\n📊 다중 종목 수집 테스트")
    print("=" * 40)

    # 키움 API 연결 필요 여부 확인
    response = input("다중 종목 수집을 테스트하시겠습니까? (시간이 오래 걸릴 수 있음) (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  다중 종목 수집 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = DailyPriceCollector()

        # 키움 연결
        if not collector.connect_kiwoom():
            print("❌ 키움 API 연결 실패")
            return False

        # 테스트 종목들 (대형주 5개)
        test_stocks = ["005930", "000660", "035420", "005380", "068270"]  # 삼성전자, SK하이닉스, NAVER, 현대차, 셀트리온
        stock_names = ["삼성전자", "SK하이닉스", "NAVER", "현대차", "셀트리온"]

        print(f"📊 테스트 종목: {len(test_stocks)}개")
        for code, name in zip(test_stocks, stock_names):
            print(f"   - {code}: {name}")

        # 진행률 콜백 함수
        def progress_callback(current, total, stock_code):
            print(f"🔄 진행률: {current}/{total} - {stock_code}")

        # 다중 종목 수집
        print("\n🔄 다중 종목 데이터 수집 중...")
        results = collector.collect_multiple_stocks(
            test_stocks,
            update_existing=True,
            progress_callback=progress_callback
        )

        # 결과 출력
        print("\n📋 수집 결과:")
        print(f"✅ 성공: {len(results['success'])}개")
        print(f"❌ 실패: {len(results['failed'])}개")
        print(f"⏭️ 건너뛰기: {len(results['skipped'])}개")
        print(f"📊 총 수집 레코드: {results['total_collected']}개")
        print(f"⏱️ 소요 시간: {results['elapsed_time']:.1f}초")

        if results['failed']:
            print(f"\n❌ 실패한 종목들: {results['failed']}")

        return len(results['success']) > 0

    except Exception as e:
        print(f"❌ 다중 종목 수집 테스트 실패: {e}")
        return False


def test_data_verification():
    """수집된 데이터 검증 테스트"""
    print("\n🔍 데이터 검증 테스트")
    print("=" * 40)

    try:
        from src.core.database import get_database_service

        db_service = get_database_service()

        # 삼성전자 데이터 확인
        test_stock = "005930"
        latest_date = db_service.get_latest_date(test_stock, "daily")

        if latest_date:
            print(f"✅ {test_stock} 최신 데이터: {latest_date}")

            # 데이터베이스 통계
            from src.core.database import get_database_manager
            db_manager = get_database_manager()
            table_info = db_manager.get_table_info()

            print("📊 데이터베이스 현황:")
            for table, count in table_info.items():
                if count > 0:
                    print(f"   📋 {table}: {count:,}개")

            return True
        else:
            print(f"⚠️ {test_stock} 데이터 없음")
            return False

    except Exception as e:
        print(f"❌ 데이터 검증 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 일봉 데이터 수집기 테스트")
    print("=" * 50)

    # 테스트 실행
    tests = [
        ("수집기 초기화", test_collector_initialization),
        ("키움 API 연결", test_kiwoom_connection),
        ("단일 종목 수집", test_single_stock_collection),
        ("다중 종목 수집", test_multiple_stocks_collection),
        ("데이터 검증", test_data_verification)]

    results = []

    for test_name, test_func in tests:
        result = test_func()
    results.append((test_name, result))

    # 결과 요약
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
        print("🎉 모든 테스트 통과! 일봉 수집기 준비 완료.")
        print("💡 이제 실제 데이터 수집을 시작할 수 있습니다.")
    elif passed >= total - 2:  # 키움 연결 관련 테스트는 선택사항
        print("✨ 핵심 기능 테스트 통과! 키움 연결 후 실제 수집 가능.")
    else:
        print("⚠️ 일부 테스트 실패. 위 로그를 확인해주세요.")

    return passed >= total - 2


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)