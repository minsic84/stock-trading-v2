"""
키움 API TR 코드 정보 관리 모듈
각 TR의 입력/출력 필드 및 메타정보 중앙 관리
"""

# TR 코드별 상세 정보
TR_INFO = {
    'opt10001': {
        'name': '주식기본정보요청',
        'description': '종목의 현재가, 등락률, 거래량 등 기본 정보 조회',
        'input_fields': {
            '종목코드': 'string'  # 6자리 종목코드
        },
        'output_fields': {
            '종목명': 'string',
            '현재가': 'int',
            '전일대비': 'int',
            '등락률': 'float',
            '거래량': 'int',
            '시가': 'int',
            '고가': 'int',
            '저가': 'int',
            '상한가': 'int',
            '하한가': 'int',
            '시가총액': 'int',
            '시가총액규모': 'string',
            '상장주수': 'int',
            'PER': 'float',
            'PBR': 'float'
        },
        'delay_ms': 3600  # API 요청 간격
    },

    'opt10081': {
        'name': '일봉차트조회',
        'description': '주식 일봉 데이터 조회',
        'input_fields': {
            '종목코드': 'string',
            '기준일자': 'string',  # YYYYMMDD
            '수정주가구분': 'string'  # 1:수정주가, 0:원주가
        },
        'output_fields': {
            '일자': 'string',
            '현재가': 'int',
            '거래량': 'int',
            '거래대금': 'int',
            '시가': 'int',
            '고가': 'int',
            '저가': 'int',
            '전일종가': 'int'
        },
        'delay_ms': 3600
    }
}


def get_tr_info(tr_code: str) -> dict:
    """TR 코드 정보 반환"""
    tr_code_lower = tr_code.lower()
    if tr_code_lower not in TR_INFO:
        raise ValueError(f"지원하지 않는 TR 코드: {tr_code}")

    info = TR_INFO[tr_code_lower].copy()
    info['code'] = tr_code_lower
    return info


def get_tr_name(tr_code: str) -> str:
    """TR 코드의 이름 반환"""
    return get_tr_info(tr_code)['name']


def get_input_fields(tr_code: str) -> dict:
    """TR 코드의 입력 필드 반환"""
    return get_tr_info(tr_code)['input_fields']


def get_output_fields(tr_code: str) -> dict:
    """TR 코드의 출력 필드 반환"""
    return get_tr_info(tr_code)['output_fields']


def get_delay_ms(tr_code: str) -> int:
    """TR 코드의 권장 딜레이 시간 반환"""
    return get_tr_info(tr_code)['delay_ms']


def validate_input_data(tr_code: str, input_data: dict) -> bool:
    """입력 데이터 유효성 검증"""
    try:
        required_fields = get_input_fields(tr_code)

        for field in required_fields:
            if field not in input_data:
                print(f"❌ 필수 입력 필드 누락: {field}")
                return False

            if not input_data[field]:
                print(f"❌ 입력 필드 값이 비어있음: {field}")
                return False

        return True

    except Exception as e:
        print(f"❌ 입력 데이터 검증 실패: {e}")
        return False


def get_all_tr_codes() -> list:
    """지원하는 모든 TR 코드 반환"""
    return list(TR_INFO.keys())


def show_tr_info(tr_code: str = None):
    """TR 코드 정보 출력 (디버깅용)"""
    if tr_code:
        try:
            info = get_tr_info(tr_code)
            print(f"🔍 TR 코드: {info['code'].upper()}")
            print(f"📝 이름: {info['name']}")
            print(f"📄 설명: {info['description']}")
            print(f"📥 입력 필드: {list(info['input_fields'].keys())}")
            print(f"📤 출력 필드: {list(info['output_fields'].keys())}")
            print(f"⏱️ 딜레이: {info['delay_ms']}ms")
        except ValueError as e:
            print(f"❌ {e}")
    else:
        print("🎯 지원하는 TR 코드 목록:")
        for code in get_all_tr_codes():
            info = TR_INFO[code]
            print(f"   📊 {code.upper()}: {info['name']}")


# 편의 함수들
def create_opt10001_input(stock_code: str) -> dict:
    """OPT10001 입력 데이터 생성"""
    return {
        '종목코드': stock_code
    }


def create_opt10081_input(stock_code: str, base_date: str = "", adj_price: str = "1") -> dict:
    """OPT10081 입력 데이터 생성"""
    return {
        '종목코드': stock_code,
        '기준일자': base_date,
        '수정주가구분': adj_price
    }


# 테스트 함수
if __name__ == "__main__":
    print("🚀 TR 코드 관리 모듈 테스트")
    print("=" * 40)

    # 전체 TR 코드 출력
    show_tr_info()

    print("\n" + "=" * 40)

    # 개별 TR 정보 출력
    show_tr_info('opt10001')

    print("\n" + "=" * 40)

    # 입력 데이터 생성 테스트
    input_data = create_opt10001_input('000200')
    print(f"🔧 OPT10001 입력 데이터: {input_data}")

    # 유효성 검증 테스트
    is_valid = validate_input_data('opt10001', input_data)
    print(f"✅ 유효성 검증: {is_valid}")