"""
한국 ETF 6종 데이터 수집 스크립트
pykrx로 KRX 공식 데이터 가져와서 JSON으로 저장
GitHub Actions에서 매일 실행
"""

from pykrx import stock
from datetime import datetime, timedelta
import json
import os
import sys

# 한국 ETF 6종 (트랙 2 운영 종목)
KOREA_ETFS = {
    '442320': {'name': 'HANARO 원자력iSelect', 'sector': '원자력'},
    '449450': {'name': 'PLUS K방산',           'sector': '방산'},
    '466820': {'name': 'TIGER 조선TOP10',      'sector': '조선'},
    '381180': {'name': 'TIGER Fn반도체TOP10',  'sector': '반도체'},
    '117700': {'name': 'KODEX 건설',           'sector': '건설'},
    '244620': {'name': 'KODEX 바이오',         'sector': '바이오'},
}

# 2년치 데이터 (주봉 EMA20 계산에 충분)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=730)
end_str = END_DATE.strftime('%Y%m%d')
start_str = START_DATE.strftime('%Y%m%d')

print(f"📅 수집 기간: {start_str} ~ {end_str}")
print(f"📊 대상: {len(KOREA_ETFS)}종 ETF\n")

result = {
    'updated_at': datetime.now().isoformat(),
    'updated_at_kst': datetime.now().strftime('%Y-%m-%d %H:%M:%S KST'),
    'period': {
        'start': start_str,
        'end': end_str,
    },
    'etfs': {},
}

success_count = 0
fail_count = 0

for code, info in KOREA_ETFS.items():
    name = info['name']
    sector = info['sector']
    print(f"🔄 {code} ({name}) 수집 중...")
    
    try:
        # 일봉 OHLCV 가져오기
        df = stock.get_etf_ohlcv_by_date(start_str, end_str, code)
        
        if df is None or len(df) == 0:
            print(f"  ❌ 데이터 없음")
            fail_count += 1
            result['etfs'][code] = {
                'name': name,
                'sector': sector,
                'error': 'no_data',
                'candles': [],
            }
            continue
        
        # JSON 형식으로 변환
        candles = []
        for date, row in df.iterrows():
            # 거래일자 형식 정규화 (Timestamp → 'YYYY-MM-DD')
            date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
            candles.append({
                'date': date_str,
                'open': int(row['시가']),
                'high': int(row['고가']),
                'low': int(row['저가']),
                'close': int(row['종가']),
                'volume': int(row['거래량']),
            })
        
        # 최근 데이터 출력
        latest = candles[-1]
        prev = candles[-2] if len(candles) > 1 else latest
        change_pct = ((latest['close'] - prev['close']) / prev['close']) * 100
        
        print(f"  ✅ {len(candles)}일 데이터")
        print(f"     최근: {latest['date']} 종가 {latest['close']:,}원 ({change_pct:+.2f}%)")
        
        result['etfs'][code] = {
            'name': name,
            'sector': sector,
            'count': len(candles),
            'candles': candles,
        }
        success_count += 1
        
    except Exception as e:
        print(f"  ❌ 오류: {e}")
        fail_count += 1
        result['etfs'][code] = {
            'name': name,
            'sector': sector,
            'error': str(e),
            'candles': [],
        }

# 저장
os.makedirs('data', exist_ok=True)
output_path = 'data/korea-etf.json'

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

# 파일 크기 확인
file_size = os.path.getsize(output_path)
print(f"\n📁 저장: {output_path} ({file_size / 1024:.1f} KB)")
print(f"🏁 완료: {success_count}/{len(KOREA_ETFS)} 성공")

# 실패 시 종료 코드
if fail_count > 0:
    print(f"⚠️ {fail_count}개 실패")
    sys.exit(1 if fail_count == len(KOREA_ETFS) else 0)
