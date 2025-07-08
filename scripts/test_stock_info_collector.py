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
from src.collectors.integrated_collector import create_integrated_collector


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
    """테스트용 종목코드 수집 (KOSPI 5개 + KOSDAQ 5개)"""
    print("\n📈 테스트용 종목코드 수집")
    print("=" * 40)

    try:
        connector = session.get_connector()
        code_collector = StockCodeCollector(connector)

        # 코스피 종목코드 수집
        print("🔄 코스피 종목코드 수집 중...")
        kospi_codes = code_collector.get_kospi_codes()

        print("🔄 코스닥 종목코드 수집 중...")
        kosdaq_codes = code_collector.get_kosdaq_codes()

        if not kospi_codes or not kosdaq_codes:
            print("❌ 종목코드 수집 실패")
            return []

        # 테스트용: KOSPI 5개 + KOSDAQ 5개 = 총 10개
        test_codes = kospi_codes[:5] + kosdaq_codes[:5]

        print(f"✅ 테스트 종목코드 준비 완료: {len(test_codes)}개")
        print("📋 테스트 종목 목록:")

        kospi_count = 0
        kosdaq_count = 0

        for i, code in enumerate(test_codes, 1):
            if code in kospi_codes:
                market = "KOSPI"
                kospi_count += 1
            else:
                market = "KOSDAQ"
                kosdaq_count += 1
            print(f"   {i:2d}. {code} ({market})")

        print(f"📊 구성: KOSPI {kospi_count}개, KOSDAQ {kosdaq_count}개")
        return test_codes

    except Exception as e:
        print(f"❌ 종목코드 수집 실패: {e}")
        return []


def test_integrated_collection(session, stock_codes):
    """통합 수집 테스트 (기본정보 + 일봉 데이터) - 10개 종목"""
    print(f"\n🚀 통합 수집 테스트 (기본정보 + 일봉)")
    print("=" * 40)

    try:
        print(f"📊 대상 종목: {len(stock_codes)}개")
        print(f"🎯 수집 목표:")
        print(f"   📋 각 종목별 기본정보 (OPT10001)")
        print(f"   📊 각 종목별 5년치 일봉 데이터")
        print(f"   🔄 누락 데이터 자동 보완")

        # 예상 소요 시간 계산
        estimated_requests = len(stock_codes) * 3  # 종목당 평균 3회 API 요청
        estimated_time = estimated_requests * 3.6 / 60  # 분 단위
        print(f"⏱️ 예상 소요시간: {estimated_time:.1f}분")

        response = input(f"\n실제 통합 수집을 시작하시겠습니까? (y/N): ")
        if response.lower() != 'y':
            print("ℹ️ 통합 수집 테스트를 건너뜁니다.")
            return True

        # 통합 수집기 생성
        print(f"\n🔧 통합 수집기 준비 중...")
        collector = create_integrated_collector(session)

        # 통합 수집 실행
        print(f"\n🔄 통합 수집 시작...")
        results = collector.collect_multiple_stocks_integrated(
            stock_codes,
            test_mode=False  # 전체 리스트 사용
        )

        # 결과 분석
        summary = results['summary']

        print(f"\n📋 통합 수집 최종 결과:")
        print(f"   📊 전체 종목: {summary['total_stocks']}개")
        print(f"   ✅ 완전 성공: {summary['success_count']}개")
        print(f"   ⚠️ 부분 성공: {summary['partial_success_count']}개")
        print(f"   ❌ 실패: {summary['failed_count']}개")

        print(f"\n📈 기본정보 수집:")
        print(f"   📥 신규 수집: {summary['total_stock_info_collected']}개")
        print(f"   🔄 업데이트: {summary['total_stock_info_updated']}개")

        print(f"\n📊 일봉 데이터:")
        print(f"   📥 수집 레코드: {summary['total_daily_records_collected']:,}개")

        print(f"\n⏱️ 실제 소요시간: {summary['elapsed_time']:.1f}초 ({summary['elapsed_time'] / 60:.1f}분)")

        # 성공한 종목들 상세 정보 (처음 3개)
        if results['success']:
            print(f"\n✅ 성공 종목 샘플:")
            for code in results['success'][:3]:
                detail = results['stock_details'][code]
                records = detail['daily_records_collected']
                elapsed = detail['elapsed_time']
                print(f"   📊 {code}: {records:,}개 레코드, {elapsed:.1f}초")

        # 실패한 종목 상세 정보
        if results['failed']:
            print(f"\n❌ 실패 종목:")
            for code in results['failed']:
                detail = results['stock_details'][code]
                error_msg = detail.get('error', '알 수 없는 오류')
                print(f"   {code}: {error_msg}")

        # 성공 여부 판단
        success_rate = summary['success_count'] / summary['total_stocks']

        if success_rate >= 0.8:  # 80% 이상 성공
            print("🎉 통합 수집 테스트 성공!")
            return True
        elif success_rate >= 0.6:  # 60% 이상 성공
            print("✨ 통합 수집 대부분 성공!")
            return True
        else:
            print("⚠️ 통합 수집 결과 미흡")
            return False

    except Exception as e:
        print(f"❌ 통합 수집 테스트 실패: {e}")
        return False


def test_database_queries():
    """데이터베이스 쿼리 테스트 및 HeidiSQL 쿼리 생성 (통합 데이터 포함)"""
    print(f"\n🔍 통합 데이터베이스 쿼리 테스트")
    print("=" * 40)

    try:
        from src.core.database import get_database_service
        db_service = get_database_service()
        db_manager = get_database_manager()

        # 기본 테이블 정보
        table_info = db_manager.get_table_info()
        print(f"📊 기본 테이블 현황:")
        for table, count in table_info.items():
            print(f"   📋 {table}: {count:,}개")

        # 일봉 테이블 확인
        print(f"\n📊 일봉 테이블 현황:")
        with db_manager.get_session() as session:
            from sqlalchemy import text

            result = session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'daily_prices_%'")
            ).fetchall()

            daily_tables = [row[0] for row in result]
            print(f"   📋 생성된 일봉 테이블: {len(daily_tables)}개")

            total_daily_records = 0
            for table in daily_tables:
                stock_code = table.replace('daily_prices_', '')
                count_result = session.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                count = count_result[0] if count_result else 0
                total_daily_records += count
                print(f"      📊 {stock_code}: {count:,}개")

            print(f"   📈 총 일봉 레코드: {total_daily_records:,}개")

        # HeidiSQL 쿼리 생성
        print(f"\n💻 HeidiSQL 확인 쿼리:")
        print(f"=" * 30)

        print(f"-- 전체 통합 현황")
        print(f"SELECT '기본정보' as type, COUNT(*) as count FROM stocks")
        print(f"UNION ALL")
        print(f"SELECT '일봉테이블' as type, COUNT(*) as count")
        print(f"FROM sqlite_master WHERE type='table' AND name LIKE 'daily_prices_%';")

        if daily_tables:
            first_table = daily_tables[0]
            stock_code = first_table.replace('daily_prices_', '')

            print(f"\n-- {stock_code} 통합 데이터 확인")
            print(f"SELECT code, name, current_price, volume, last_updated")
            print(f"FROM stocks WHERE code = '{stock_code}';")
            print(f"")
            print(f"SELECT date, close_price, volume, data_source")
            print(f"FROM {first_table}")
            print(f"ORDER BY date DESC LIMIT 10;")

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 쿼리 테스트 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 통합 수집 시스템 테스트 (기본정보 + 일봉)")
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

        # 4단계: 통합 수집 테스트 ⭐ 핵심
        integrated_success = test_integrated_collection(session, test_codes)
        results.append(("통합 수집", integrated_success))

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
        print("💡 통합 수집 완료! 이제 5년치 주식 데이터를 자동 수집할 수 있습니다.")
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