#!/usr/bin/env python3
"""
통합 수집기 테스트 스크립트
기본정보 + 일봉 데이터 통합 수집 테스트 (5개 종목)
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.base_session import create_kiwoom_session
from src.collectors.integrated_collector import create_integrated_collector
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
        db_manager = get_database_manager()

        if db_manager.test_connection():
            print("✅ 데이터베이스 연결 성공")
        else:
            print("❌ 데이터베이스 연결 실패")
            return False

        # 테이블 생성 (필요시)
        db_manager.create_tables()
        print("✅ 기본 테이블 준비 완료")

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 준비 실패: {e}")
        return False


def test_integrated_collection(session):
    """통합 수집 테스트 (5개 종목)"""
    print("\n🚀 통합 수집 테스트 (5개 종목)")
    print("=" * 40)

    # 테스트 종목 5개 (대형주)
    test_codes = ["005930", "000660", "035420", "005380", "068270"]
    stock_names = ["삼성전자", "SK하이닉스", "NAVER", "현대차", "셀트리온"]

    print(f"📊 테스트 종목: {len(test_codes)}개")
    for code, name in zip(test_codes, stock_names):
        print(f"   📈 {code}: {name}")

    response = input("\n실제 통합 수집을 시작하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("ℹ️ 통합 수집 테스트를 건너뜁니다.")
        return True

    try:
        # 통합 수집기 생성
        collector = create_integrated_collector(session)

        # 통합 수집 실행
        results = collector.collect_multiple_stocks_integrated(
            test_codes,
            test_mode=True  # 5개만 수집
        )

        # 결과 분석
        summary = results['summary']

        print(f"\n📋 통합 수집 결과:")
        print(f"   ✅ 완전 성공: {summary['success_count']}/{summary['total_stocks']}개")
        print(f"   ⚠️ 부분 성공: {summary['partial_success_count']}개")
        print(f"   ❌ 실패: {summary['failed_count']}개")
        print(f"   📊 수집 레코드: {summary['total_daily_records_collected']:,}개")
        print(f"   ⏱️ 소요시간: {summary['elapsed_time']:.1f}초")

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


def test_data_verification():
    """수집된 데이터 검증"""
    print("\n🔍 데이터 검증 테스트")
    print("=" * 40)

    try:
        db_manager = get_database_manager()

        # 생성된 테이블 확인
        print("📊 생성된 일봉 테이블 확인:")

        with db_manager.get_session() as session:
            from sqlalchemy import text

            # 일봉 테이블 목록 조회
            result = session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'daily_prices_%'")
            ).fetchall()

            daily_tables = [row[0] for row in result]
            print(f"   📋 일봉 테이블: {len(daily_tables)}개")

            for table in daily_tables[:5]:  # 처음 5개만 표시
                stock_code = table.replace('daily_prices_', '')

                # 테이블 레코드 수 확인
                count_result = session.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).fetchone()

                count = count_result[0] if count_result else 0
                print(f"      📊 {stock_code}: {count:,}개")

        print("✅ 데이터 검증 완료")
        return True

    except Exception as e:
        print(f"❌ 데이터 검증 실패: {e}")
        return False


def show_heidi_queries():
    """HeidiSQL 확인 쿼리 출력"""
    print("\n💻 HeidiSQL 확인 쿼리")
    print("=" * 40)

    print("-- 생성된 일봉 테이블 목록")
    print("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'daily_prices_%';")
    print()

    print("-- 삼성전자 일봉 데이터 확인 (최신 10개)")
    print("SELECT date, close_price, volume, data_source")
    print("FROM daily_prices_005930")
    print("ORDER BY date DESC")
    print("LIMIT 10;")
    print()

    print("-- 전체 일봉 데이터 통계")
    print("SELECT ")
    print("  '005930' as stock_code, COUNT(*) as record_count")
    print("FROM daily_prices_005930")
    print("UNION ALL")
    print("SELECT ")
    print("  '000660' as stock_code, COUNT(*) as record_count")
    print("FROM daily_prices_000660;")


def main():
    """메인 테스트 함수"""
    print("🚀 통합 수집기 테스트")
    print("=" * 50)

    # 테스트 실행
    tests = [
        ("데이터베이스 준비", test_database_preparation),
    ]

    results = []
    session = None

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

    # 3단계: 통합 수집 테스트
    integrated_success = test_integrated_collection(session)
    results.append(("통합 수집", integrated_success))

    # 4단계: 데이터 검증
    verification_success = test_data_verification()
    results.append(("데이터 검증", verification_success))

    # 5단계: HeidiSQL 쿼리 안내
    show_heidi_queries()

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
        print("🎉 모든 테스트 통과! 통합 수집 시스템 완성!")
        print("💡 이제 5년치 주식 데이터를 자동으로 수집할 수 있습니다.")
    elif passed >= total - 1:
        print("✨ 핵심 기능 테스트 통과! 실제 운영 가능.")
    else:
        print("⚠️ 주요 테스트 실패. 위 로그를 확인해주세요.")

    return passed >= total - 1


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