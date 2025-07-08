"""
데이터 변환 유틸리티
- OPT10001 기본정보 → daily_prices 테이블 형태 변환
- 당일 데이터 보완 로직
- 종목별 일봉 테이블 자동 생성
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import sys
from pathlib import Path
from sqlalchemy import text, Column, Integer, String, BigInteger, DateTime, Index, UniqueConstraint

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_database_manager, Base
from src.utils.trading_date import get_market_today

logger = logging.getLogger(__name__)


class StockDailyTable(Base):
    """동적 종목별 일봉 테이블 모델"""
    __abstract__ = True  # 추상 클래스로 설정

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(8), nullable=False, comment='일자(YYYYMMDD)')

    # 가격 정보
    open_price = Column(Integer, comment='시가')
    high_price = Column(Integer, comment='고가')
    low_price = Column(Integer, comment='저가')
    close_price = Column(Integer, comment='종가/현재가')

    # 거래 정보
    volume = Column(BigInteger, comment='거래량')
    trading_value = Column(BigInteger, comment='거래대금')

    # 변동 정보
    prev_day_diff = Column(Integer, comment='전일대비', default=0)
    change_rate = Column(Integer, comment='등락율(소수점2자리*100)', default=0)

    # 메타 정보
    data_source = Column(String(20), comment='데이터 출처 (OPT10001/OPT10081)')
    created_at = Column(DateTime, default=datetime.now, comment='생성일시')


class DataConverter:
    """데이터 변환 및 테이블 관리 클래스"""

    def __init__(self):
        self.db_manager = get_database_manager()
        logger.info("데이터 변환기 초기화 완료")

    def create_daily_table_for_stock(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 생성"""
        try:
            table_name = f"daily_prices_{stock_code}"

            # 이미 존재하는지 확인
            if self._table_exists(table_name):
                logger.info(f"{stock_code}: 테이블 {table_name} 이미 존재")
                return True

            # 동적 테이블 생성 SQL
            create_sql = f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT(8) NOT NULL,
                open_price INTEGER,
                high_price INTEGER,
                low_price INTEGER,
                close_price INTEGER,
                volume BIGINT,
                trading_value BIGINT,
                prev_day_diff INTEGER DEFAULT 0,
                change_rate INTEGER DEFAULT 0,
                data_source TEXT(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """

            # 인덱스 생성 SQL
            index_sql = f"""
            CREATE UNIQUE INDEX idx_{table_name}_date ON {table_name}(date)
            """

            with self.db_manager.get_session() as session:
                # 테이블 생성
                session.execute(text(create_sql))
                session.execute(text(index_sql))
                session.commit()

            logger.info(f"{stock_code}: 일봉 테이블 {table_name} 생성 완료")
            return True

        except Exception as e:
            logger.error(f"{stock_code}: 테이블 생성 실패 - {e}")
            return False

    def convert_stock_info_to_daily(self, stock_code: str) -> bool:
        """
        주식 기본정보(OPT10001)를 일봉 데이터로 변환하여 저장

        Args:
            stock_code: 종목코드

        Returns:
            bool: 변환 성공 여부
        """
        try:
            logger.info(f"{stock_code}: 기본정보 → 일봉 데이터 변환 시작")

            # 1. 테이블 생성 (필요시)
            if not self.create_daily_table_for_stock(stock_code):
                return False

            # 2. 기본정보에서 데이터 조회
            stock_info = self._get_stock_basic_info(stock_code)
            if not stock_info:
                logger.error(f"{stock_code}: 기본정보 조회 실패")
                return False

            # 3. 일봉 형태로 변환
            daily_data = self._convert_to_daily_format(stock_info)
            if not daily_data:
                logger.error(f"{stock_code}: 일봉 형태 변환 실패")
                return False

            # 4. 일봉 테이블에 저장
            success = self._save_daily_data(stock_code, daily_data)

            if success:
                logger.info(f"{stock_code}: 당일 데이터 변환 완료")
            else:
                logger.error(f"{stock_code}: 일봉 데이터 저장 실패")

            return success

        except Exception as e:
            logger.error(f"{stock_code}: 데이터 변환 중 오류 - {e}")
            return False

    def _table_exists(self, table_name: str) -> bool:
        """테이블 존재 여부 확인"""
        try:
            with self.db_manager.get_session() as session:
                result = session.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"),
                    {"table_name": table_name}
                ).fetchone()
                return result is not None

        except Exception as e:
            logger.error(f"테이블 존재 확인 실패: {e}")
            return False

    def _get_stock_basic_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """stocks 테이블에서 기본정보 조회"""
        try:
            from src.core.database import Stock

            with self.db_manager.get_session() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()

                if not stock:
                    logger.error(f"{stock_code}: stocks 테이블에 데이터 없음")
                    return None

                # 필요한 필드들 추출
                stock_data = {
                    'code': stock.code,
                    'name': stock.name,
                    'current_price': stock.current_price,
                    'prev_day_diff': stock.prev_day_diff,
                    'change_rate': stock.change_rate,
                    'volume': stock.volume,
                    'open_price': stock.open_price,
                    'high_price': stock.high_price,
                    'low_price': stock.low_price,
                    'last_updated': stock.last_updated
                }

                logger.debug(f"{stock_code}: 기본정보 조회 완료 - 현재가: {stock.current_price}")
                return stock_data

        except Exception as e:
            logger.error(f"{stock_code}: 기본정보 조회 실패 - {e}")
            return None

    def _convert_to_daily_format(self, stock_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """기본정보를 일봉 형태로 변환"""
        try:
            # 시장 기준 오늘 날짜
            today = get_market_today()
            today_str = today.strftime('%Y%m%d')

            # 기본정보 → 일봉 매핑
            daily_data = {
                'date': today_str,
                'open_price': stock_info.get('open_price', 0),
                'high_price': stock_info.get('high_price', 0),
                'low_price': stock_info.get('low_price', 0),
                'close_price': stock_info.get('current_price', 0),  # 현재가 → 종가
                'volume': stock_info.get('volume', 0),
                'trading_value': 0,  # 기본정보에는 거래대금 없음
                'prev_day_diff': stock_info.get('prev_day_diff', 0),
                'change_rate': stock_info.get('change_rate', 0),
                'data_source': 'OPT10001'
            }

            # 데이터 유효성 검증
            if daily_data['close_price'] <= 0:
                logger.warning(f"현재가가 0 이하: {daily_data['close_price']}")
                return None

            logger.debug(f"일봉 변환 완료: {today_str} - {daily_data['close_price']:,}원")
            return daily_data

        except Exception as e:
            logger.error(f"일봉 형태 변환 실패: {e}")
            return None

    def _save_daily_data(self, stock_code: str, daily_data: Dict[str, Any]) -> bool:
        """일봉 데이터를 종목별 테이블에 저장"""
        try:
            table_name = f"daily_prices_{stock_code}"

            # INSERT OR REPLACE 쿼리 (중복 날짜 처리)
            insert_sql = f"""
            INSERT OR REPLACE INTO {table_name} 
            (date, open_price, high_price, low_price, close_price, 
             volume, trading_value, prev_day_diff, change_rate, data_source, created_at)
            VALUES 
            (:date, :open_price, :high_price, :low_price, :close_price,
             :volume, :trading_value, :prev_day_diff, :change_rate, :data_source, :created_at)
            """

            # 현재 시간 추가
            daily_data['created_at'] = datetime.now()

            with self.db_manager.get_session() as session:
                session.execute(text(insert_sql), daily_data)
                session.commit()

            logger.info(f"{stock_code}: 일봉 데이터 저장 완료 - {daily_data['date']}")
            return True

        except Exception as e:
            logger.error(f"{stock_code}: 일봉 데이터 저장 실패 - {e}")
            return False

    def test_converter(self, test_codes: list = None):
        """데이터 변환기 테스트"""
        if test_codes is None:
            test_codes = ["005930"]  # 삼성전자로 테스트

        print("🔄 데이터 변환기 테스트")
        print("=" * 40)

        for stock_code in test_codes:
            print(f"\n📊 {stock_code} 변환 테스트 중...")

            # 1. 테이블 생성 테스트
            print(f"🏗️ 테이블 생성 중...")
            table_success = self.create_daily_table_for_stock(stock_code)
            print(f"   테이블 생성: {'✅ 성공' if table_success else '❌ 실패'}")

            if not table_success:
                continue

            # 2. 데이터 변환 테스트
            print(f"🔄 데이터 변환 중...")
            convert_success = self.convert_stock_info_to_daily(stock_code)
            print(f"   데이터 변환: {'✅ 성공' if convert_success else '❌ 실패'}")

            # 3. 결과 확인
            if convert_success:
                self._verify_conversion_result(stock_code)

        print(f"\n✅ 데이터 변환기 테스트 완료!")

    def _verify_conversion_result(self, stock_code: str):
        """변환 결과 검증"""
        try:
            table_name = f"daily_prices_{stock_code}"

            with self.db_manager.get_session() as session:
                result = session.execute(
                    text(f"SELECT date, close_price, volume, data_source FROM {table_name} ORDER BY date DESC LIMIT 1")
                ).fetchone()

                if result:
                    print(f"   📊 최신 데이터: {result[0]} - {result[1]:,}원 (출처: {result[3]})")
                else:
                    print(f"   ❌ 저장된 데이터 없음")

        except Exception as e:
            print(f"   ❌ 결과 확인 실패: {e}")


def get_data_converter() -> DataConverter:
    """데이터 변환기 인스턴스 반환 (편의 함수)"""
    return DataConverter()


# 편의 함수들
def create_daily_table(stock_code: str) -> bool:
    """종목별 일봉 테이블 생성 (편의 함수)"""
    return get_data_converter().create_daily_table_for_stock(stock_code)


def convert_today_data(stock_code: str) -> bool:
    """당일 데이터 변환 (편의 함수)"""
    return get_data_converter().convert_stock_info_to_daily(stock_code)


# 직접 실행 시 테스트
if __name__ == "__main__":
    converter = DataConverter()
    converter.test_converter(["005930", "000660"])