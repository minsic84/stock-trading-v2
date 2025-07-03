#!/usr/bin/env python3
"""
일봉 데이터 수집 예제 스크립트
실제 데이터 수집을 위한 사용 예제
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.daily_price import (
    DailyPriceCollector,
    collect_daily_price_single,
    collect_daily_price_batch,
    collect_market_daily_prices
)


def example_single_stock():
    """단일 종목 수집 예제"""
    print("📈 단일 종목 일봉 데이터 수집 예제")
    print("=" * 45)

    # 삼성전자 일봉 데이터 수집
    stock_code = "005930"
    print(f"🔄 {stock_code} (삼성전자) 일봉 데이터 수집 중...")

    success = collect_daily_price_single(stock_code)

    if success:
        print("✅ 수집 완료!")
    else:
        print("❌ 수집 실패")


def example_multiple_stocks():
    """다중 종목 수집 예제"""
    print("\n📊 다중 종목 일봉 데이터 수집 예제")
    print("=" * 45)

    # 대형주 종목들
    stock_codes = [
        "005930",  # 삼성전자
        "000660",  # SK하이닉스
        "035420",  # NAVER
        "005380",  # 현대차
        "068270"  # 셀트리온
    ]

    print(f"🔄 {len(stock_codes)}개 종목 일봉 데이터 수집 중...")

    results = collect_daily_price_batch(stock_codes)

    if 'error' in results:
        print(f"❌ 수집 실패: {results['error']}")
    else:
        print("📋 수집 결과:")
        print(f"  ✅ 성공: {len(results['success'])}개")
        print(f"  ❌ 실패: {len(results['failed'])}개")
        print(f"  📊 총 레코드: {results['total_collected']}개")
        print(f"  ⏱️ 소요시간: {results['elapsed_time']:.1f}초")


def example_custom_collector():
    """커스텀 수집기 사용 예제"""
    print("\n🔧 커스텀 수집기 사용 예제")
    print("=" * 45)

    # 수집기 인스턴스 생성
    collector = DailyPriceCollector()

    # 키움 API 연결
    if not collector.connect_kiwoom():
        print("❌ 키움 API 연결 실패")
        return

    print("✅ 키움 API 연결 성공")

    # 특정 기간 데이터 수집
    stock_code = "005930"
    start_date = "20241101"  # 2024년 11월 1일부터
    end_date = "20241201"  # 2024년 12월 1일까지

    print(f"🔄 {stock_code} 기간별 데이터 수집 ({start_date} ~ {end_date})")

    success = collector.collect_single_stock(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        update_existing=True
    )

    if success:
        status = collector.get_collection_status()
        print(f"✅ 수집 완료: {status['collected_count']}개 레코드")
    else:
        print("❌ 수집 실패")


def example_market_collection():
    """전체 시장 수집 예제 (주의: 시간이 매우 오래 걸림)"""
    print("\n🏢 전체 시장 데이터 수집 예제")
    print("=" * 45)

    response = input("⚠️ 전체 시장 수집은 몇 시간이 걸릴 수 있습니다. 계속하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("ℹ️ 전체 시장 수집을 건너뜁니다.")
        return

    # KOSPI 시장만 수집 (KOSDAQ은 더 오래 걸림)
    print("🔄 KOSPI 시장 전체 종목 수집 중...")

    results = collect_market_daily_prices(market="KOSPI")

    if 'error' in results:
        print(f"❌ 수집 실패: {results['error']}")
    else:
        print("📋 KOSPI 수집 결과:")
        print(f"  ✅ 성공: {len(results['success'])}개")
        print(f"  ❌ 실패: {len(results['failed'])}개")
        print(f"  📊 총 레코드: {results['total_collected']:,}개")
        print(f"  ⏱️ 소요시간: {results['elapsed_time']:.1f}초")


def example_progress_tracking():
    """진행률 추적 예제"""
    print("\n📊 진행률 추적 수집 예제")
    print("=" * 45)

    collector = DailyPriceCollector()

    if not collector.connect_kiwoom():
        print("❌ 키움 API 연결 실패")
        return

    # 테스트 종목들
    stock_codes = ["005930", "000660", "035420", "005380", "068270"]

    def progress_callback(current, total, stock_code):
        """진행률 출력 콜백"""
        progress = (current / total) * 100
        print(f"📈 진행률: {progress:5.1f}% ({current:2d}/{total}) - {stock_code}")

    print(f"🔄 {len(stock_codes)}개 종목 수집 (진행률 표시)")

    results = collector.collect_multiple_stocks(
        stock_codes,
        progress_callback=progress_callback
    )

    print(f"\n✅ 수집 완료: {len(results['success'])}개 성공")


def main():
    """메인 함수"""
    print("🚀 일봉 데이터 수집 예제 모음")
    print("=" * 50)

    examples = [
        ("1. 단일 종목 수집", example_single_stock),
        ("2. 다중 종목 수집", example_multiple_stocks),
        ("3. 커스텀 수집기", example_custom_collector),
        ("4. 진행률 추적", example_progress_tracking),
        ("5. 전체 시장 수집", example_market_collection),
    ]

    print("📋 사용 가능한 예제들:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"   {name}")

    print("\n⚠️ 주의사항:")
    print("   - 키움증권 OpenAPI 로그인이 필요합니다")
    print("   - API 요청 제한으로 인해 시간이 걸릴 수 있습니다")
    print("   - 장 운영시간에는 실시간 데이터로 인해 오류가 발생할 수 있습니다")

    # 사용자 선택
    try:
        choice = input("\n실행할 예제 번호를 선택하세요 (1-5, 0=전체): ")

        if choice == "0":
            # 전체 실행 (5번 제외)
            for name, func in examples[:-1]:
                print(f"\n{'=' * 20} {name} {'=' * 20}")
                func()
        elif choice in ["1", "2", "3", "4", "5"]:
            idx = int(choice) - 1
            name, func = examples[idx]
            print(f"\n{'=' * 20} {name} {'=' * 20}")
            func()
        else:
            print("❌ 잘못된 선택입니다.")

    except KeyboardInterrupt:
        print("\n\n👋 사용자가 중단했습니다.")
    except Exception as e:
        print(f"\n❌ 실행 중 오류 발생: {e}")


if __name__ == "__main__":
    main()