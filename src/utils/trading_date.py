"""
한국 주식시장 거래일 계산 유틸리티
- 주말/공휴일 제외한 거래일만 계산
- 장 시작 전 시간 고려 ("오늘" 정의)
- 누락된 거래일 수 정확한 계산
"""
import logging
from datetime import date, datetime, timedelta
from typing import List, Tuple

logger = logging.getLogger(__name__)


class TradingDateCalculator:
    """한국 주식시장 거래일 계산기"""

    def __init__(self):
        # 2025년 한국 공휴일 (고정 공휴일)
        self.holidays_2025 = [
            date(2025, 1, 1),   # 신정
            date(2025, 1, 28),  # 설날 연휴
            date(2025, 1, 29),  # 설날
            date(2025, 1, 30),  # 설날 연휴
            date(2025, 3, 1),   # 삼일절
            date(2025, 5, 5),   # 어린이날
            date(2025, 5, 6),   # 어린이날 대체공휴일
            date(2025, 6, 6),   # 현충일
            date(2025, 8, 15),  # 광복절
            date(2025, 10, 3),  # 개천절
            date(2025, 10, 6),  # 개천절 대체공휴일
            date(2025, 10, 9),  # 한글날
            date(2025, 12, 25), # 성탄절
        ]

    def get_market_today(self) -> date:
        """
        시장 기준 "오늘" 날짜 반환
        장 시작 전(09:00 이전)이면 전날이 "오늘"
        """
        now = datetime.now()

        if now.hour < 9:  # 09:00 이전
            market_today = (now - timedelta(days=1)).date()
            logger.debug(f"장 시작 전: {now.strftime('%H:%M')} → 시장 오늘: {market_today}")
        else:
            market_today = now.date()
            logger.debug(f"장 시간 중/후: {now.strftime('%H:%M')} → 시장 오늘: {market_today}")

        return market_today

    def is_trading_day(self, target_date: date) -> bool:
        """해당 날짜가 거래일인지 확인"""
        # 주말 체크 (토요일: 5, 일요일: 6)
        if target_date.weekday() >= 5:
            return False

        # 공휴일 체크
        if target_date in self.holidays_2025:
            return False

        return True

    def get_last_trading_day(self, base_date: date = None) -> date:
        """최근 거래일 반환"""
        if base_date is None:
            base_date = self.get_market_today()

        current_date = base_date

        # 최대 14일 전까지 검색
        for i in range(14):
            if self.is_trading_day(current_date):
                return current_date
            current_date -= timedelta(days=1)

        # 14일 내에 거래일이 없으면 기준일 반환
        logger.warning(f"14일 내 거래일 없음. 기준일 반환: {base_date}")
        return base_date

    def get_trading_days_between(self, start_date: date, end_date: date) -> List[date]:
        """두 날짜 사이의 모든 거래일 반환 (start_date 포함, end_date 제외)"""
        trading_days = []
        current_date = start_date

        while current_date < end_date:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        return trading_days

    def count_missing_trading_days(self, last_data_date: str, target_date: date = None) -> Tuple[int, List[date]]:
        """
        마지막 데이터 날짜부터 목표 날짜까지 누락된 거래일 수 계산

        Args:
            last_data_date: 마지막 데이터 날짜 (YYYYMMDD 형태)
            target_date: 목표 날짜 (기본값: 시장 기준 오늘)

        Returns:
            (누락된 거래일 수, 누락된 날짜 리스트)
        """
        try:
            # 문자열을 date 객체로 변환
            last_date = datetime.strptime(last_data_date, '%Y%m%d').date()

            if target_date is None:
                target_date = self.get_market_today()

            # 마지막 데이터 다음날부터 목표일까지의 거래일 계산
            next_day = last_date + timedelta(days=1)
            missing_dates = self.get_trading_days_between(next_day, target_date + timedelta(days=1))

            logger.info(f"누락 기간: {last_data_date} → {target_date}, 누락 거래일: {len(missing_dates)}개")

            return len(missing_dates), missing_dates

        except Exception as e:
            logger.error(f"누락 거래일 계산 실패: {e}")
            return 0, []

    def test_calculator(self):
        """거래일 계산기 테스트"""
        print("📅 거래일 계산기 테스트")
        print("=" * 40)

        # 1. 시장 기준 오늘 확인
        market_today = self.get_market_today()
        actual_today = datetime.now().date()
        current_time = datetime.now().strftime('%H:%M')

        print(f"🕐 현재 시간: {current_time}")
        print(f"📅 실제 오늘: {actual_today}")
        print(f"📊 시장 오늘: {market_today}")

        if market_today != actual_today:
            print("ℹ️  장 시작 전이므로 전날이 시장 기준 오늘입니다.")

        # 2. 최근 거래일 확인
        last_trading = self.get_last_trading_day()
        print(f"📈 최근 거래일: {last_trading}")

        # 3. 거래일 여부 확인
        print(f"\n📋 최근 일주일 거래일 확인:")
        for i in range(7):
            check_date = market_today - timedelta(days=i)
            is_trading = self.is_trading_day(check_date)
            weekday = ['월', '화', '수', '목', '금', '토', '일'][check_date.weekday()]
            status = "✅ 거래일" if is_trading else "❌ 휴장일"
            print(f"   {check_date} ({weekday}): {status}")

        # 4. 누락 계산 테스트
        print(f"\n🔍 누락 거래일 계산 테스트:")
        test_cases = [
            "20250701",  # 1주일 전
            "20250620",  # 2주일 전
            "20250601",  # 한 달 전
            "20241201",  # 반년 전
        ]

        for test_date in test_cases:
            count, dates = self.count_missing_trading_days(test_date)
            print(f"   {test_date} → {market_today}: {count}개 누락")
            if count <= 10:  # 10개 이하면 상세 표시
                print(f"      누락 날짜: {[d.strftime('%m-%d') for d in dates]}")

        print(f"\n✅ 거래일 계산기 테스트 완료!")


def get_trading_calculator() -> TradingDateCalculator:
    """거래일 계산기 인스턴스 반환 (편의 함수)"""
    return TradingDateCalculator()


# 편의 함수들
def get_market_today() -> date:
    """시장 기준 오늘 날짜 (편의 함수)"""
    return get_trading_calculator().get_market_today()


def count_missing_days(last_data_date: str) -> Tuple[int, List[date]]:
    """누락된 거래일 수 계산 (편의 함수)"""
    return get_trading_calculator().count_missing_trading_days(last_data_date)


def is_trading_day(target_date: date) -> bool:
    """거래일 여부 확인 (편의 함수)"""
    return get_trading_calculator().is_trading_day(target_date)


# 직접 실행 시 테스트
if __name__ == "__main__":
    calculator = TradingDateCalculator()
    calculator.test_calculator()