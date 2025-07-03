# utils/trading_date.py
from datetime import date, timedelta
from typing import List, Optional
import logging


class TradingDateCalculator:
    """
    한국 주식시장 영업일 계산기
    - 주말 제외
    - 공휴일 제외 (주요 공휴일)
    - 최근 거래일 자동 계산
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def get_korean_holidays(year: int) -> List[date]:
        """한국 주요 공휴일 반환 (고정 공휴일만)"""
        holidays = [
            # 신정
            date(year, 1, 1),

            # 삼일절
            date(year, 3, 1),

            # 어린이날
            date(year, 5, 5),

            # 현충일
            date(year, 6, 6),

            # 광복절
            date(year, 8, 15),

            # 개천절
            date(year, 10, 3),

            # 한글날
            date(year, 10, 9),

            # 성탄절
            date(year, 12, 25),

            # 2025년 추가 공휴일 (예시)
            date(2025, 1, 28),  # 설날 연휴 시작
            date(2025, 1, 29),  # 설날
            date(2025, 1, 30),  # 설날 연휴 끝

            date(2025, 5, 6),  # 어린이날 대체공휴일

            date(2025, 10, 6),  # 개천절 대체공휴일
        ]

        return holidays

    def is_trading_day(self, target_date: date) -> bool:
        """해당 날짜가 거래일인지 확인"""
        # 주말 체크 (토: 5, 일: 6)
        if target_date.weekday() >= 5:
            return False

        # 공휴일 체크
        holidays = self.get_korean_holidays(target_date.year)
        if target_date in holidays:
            return False

        return True

    def get_last_trading_day(self, base_date: date = None) -> date:
        """최근 거래일 반환"""
        if base_date is None:
            base_date = date.today()

        current_date = base_date

        # 최대 14일 전까지 검색 (2주)
        for i in range(14):
            current_date = base_date - timedelta(days=i)

            if self.is_trading_day(current_date):
                self.logger.info(f"최근 거래일: {current_date}")
                return current_date

        # 14일 내에 거래일이 없으면 경고
        self.logger.warning(f"최근 14일 내 거래일을 찾을 수 없습니다. 기준일: {base_date}")
        return base_date - timedelta(days=1)

    def get_previous_trading_day(self, target_date: date) -> date:
        """특정 날짜의 이전 거래일 반환"""
        current_date = target_date - timedelta(days=1)

        # 최대 10일 전까지 검색
        for i in range(10):
            if self.is_trading_day(current_date):
                return current_date
            current_date -= timedelta(days=1)

        # 10일 내에 없으면 그냥 전일 반환
        return target_date - timedelta(days=1)

    def get_next_trading_day(self, target_date: date) -> date:
        """특정 날짜의 다음 거래일 반환"""
        current_date = target_date + timedelta(days=1)

        # 최대 10일 후까지 검색
        for i in range(10):
            if self.is_trading_day(current_date):
                return current_date
            current_date += timedelta(days=1)

        # 10일 내에 없으면 그냥 다음일 반환
        return target_date + timedelta(days=1)

    def get_trading_days_between(self, start_date: date, end_date: date) -> List[date]:
        """두 날짜 사이의 모든 거래일 반환"""
        trading_days = []
        current_date = start_date

        while current_date <= end_date:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        return trading_days

    def is_market_open_time(self) -> bool:
        """현재 시간이 장 운영시간인지 확인 (9:00 ~ 15:30)"""
        from datetime import datetime

        now = datetime.now()

        # 거래일이 아니면 False
        if not self.is_trading_day(now.date()):
            return False

        # 장 운영시간 체크 (9:00 ~ 15:30)
        market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_open <= now <= market_close

    def get_trading_date_info(self, target_date: date = None) -> dict:
        """거래일 관련 종합 정보 반환"""
        if target_date is None:
            target_date = date.today()

        info = {
            'target_date': target_date,
            'is_trading_day': self.is_trading_day(target_date),
            'is_weekend': target_date.weekday() >= 5,
            'is_holiday': target_date in self.get_korean_holidays(target_date.year),
            'last_trading_day': self.get_last_trading_day(target_date),
            'previous_trading_day': self.get_previous_trading_day(target_date),
            'next_trading_day': self.get_next_trading_day(target_date),
            'is_market_open': self.is_market_open_time() if target_date == date.today() else False
        }

        return info


def test_trading_date():
    """거래일 계산기 테스트"""
    print("📅 한국 주식시장 영업일 계산기 테스트")
    print("=" * 50)

    calculator = TradingDateCalculator()

    # 현재 날짜 정보
    today = date.today()
    info = calculator.get_trading_date_info(today)

    print(f"📊 오늘 날짜 정보: {today}")
    print(f"거래일 여부: {'✅' if info['is_trading_day'] else '❌'}")
    print(f"주말 여부: {'✅' if info['is_weekend'] else '❌'}")
    print(f"공휴일 여부: {'✅' if info['is_holiday'] else '❌'}")
    print(f"장 운영시간: {'✅' if info['is_market_open'] else '❌'}")

    print(f"\n📅 거래일 정보:")
    print(f"최근 거래일: {info['last_trading_day']}")
    print(f"이전 거래일: {info['previous_trading_day']}")
    print(f"다음 거래일: {info['next_trading_day']}")

    # 최근 5일 거래일 확인
    print(f"\n📈 최근 거래일들:")
    base_date = info['last_trading_day']
    for i in range(5):
        trading_day = calculator.get_previous_trading_day(base_date) if i > 0 else base_date
        base_date = trading_day
        weekday_name = ['월', '화', '수', '목', '금', '토', '일'][trading_day.weekday()]
        print(f"  {trading_day} ({weekday_name})")

    # 2025년 주요 공휴일
    print(f"\n🎊 2025년 주요 공휴일:")
    holidays = calculator.get_korean_holidays(2025)
    for holiday in sorted(holidays):
        weekday_name = ['월', '화', '수', '목', '금', '토', '일'][holiday.weekday()]
        print(f"  {holiday} ({weekday_name})")

    print(f"\n✅ 테스트 완료!")
    print(f"💡 데이터 수집 권장 날짜: {info['last_trading_day']}")


if __name__ == "__main__":
    test_trading_date()