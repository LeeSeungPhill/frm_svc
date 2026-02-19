import ccxt
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
import pytz
import schedule
import slack_sdk
from slack_sdk.errors import SlackApiError
from scipy.stats import linregress
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

# ATR% 임계값 (변동성 → trend_type 매핑)
ATR_THRESHOLD_HIGH = 6.0   # short
ATR_THRESHOLD_MID = 3.5    # mid
ATR_THRESHOLD_LOW = 1.5    # long (미만은 watch)

REENTRY_HOURS = {'short': 4, 'mid': 8, 'long': 16}

# 업비트 API 키 설정
API_KEY = os.environ['UPBIT_ACCESS_KEY']
SECRET_KEY = os.environ['UPBIT_SECRET_KEY']

# 업비트 거래소 초기화
exchange = ccxt.upbit({
    'apiKey': API_KEY,
    'secret': SECRET_KEY
})

# Slack 메세지 연동
SLACK_BOT_TOKEN1 = os.environ['SLACK_BOT_TOKEN1']
SLACK_BOT_TOKEN2 = os.environ['SLACK_BOT_TOKEN2']
SLACK_BOT_TOKEN3 = os.environ['SLACK_BOT_TOKEN3']
SLACK_BOT_TOKEN4 = os.environ['SLACK_BOT_TOKEN4']
client = slack_sdk.WebClient(token=SLACK_BOT_TOKEN1+SLACK_BOT_TOKEN2+SLACK_BOT_TOKEN3+SLACK_BOT_TOKEN4)

# 전송된 메시지 기록 저장 (전역 변수)
sent_messages = set()

# 데이터베이스 연결 정보
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

def get_trend_line(dates, prices):
    """
    날짜와 가격 데이터를 받아 선형 회귀를 통해 추세선을 생성.
    데이터 포인트가 2개 미만이면 (None, None) 반환.
    """
    if len(dates) < 2 or len(prices) < 2:
        return None, None
    # x = np.array([(date - min(dates)).days for date in dates])    # 일단위
    x = np.array([(date - min(dates)).total_seconds() / 60 for date in dates])  # 15분 단위
    y = np.array(prices)
    slope, intercept, _, _, _ = linregress(x, y)
    return slope, intercept

def predict_price(date, min_date, slope, intercept):
    """
    주어진 날짜에 대한 예상 가격을 반환.
    """
    # x = (date - min_date).days    # 일단위
    x = (date - min_date).total_seconds() / 60  # 15분 단위
    return slope * x + intercept

def get_highs_lows(df, trend_type):
    """
    데이터프레임에서 단기, 중기, 장기 추세별 고점과 저점을 추출하여 반환.
    """

    # 추세 유형에 따른 데이터 범위 설정
    if trend_type == 'short':   # 단기 추세 (30일)
        recent_data = df.tail(30)
        window_size = 5         # 민감한 탐지를 위해 작은 윈도우 사용
    elif trend_type == 'long':  # 장기 추세 (180일)
        recent_data = df.tail(180)
        window_size = 15        # 더 부드럽게 장기 추세를 포착
    else:                       # 중기 추세 (90일, 기본값)
        recent_data = df.tail(90)
        window_size = 9         # 균형 잡힌 추세 탐지

    rolling_high = recent_data['high'].rolling(window_size, center=True).max()
    rolling_low = recent_data['low'].rolling(window_size, center=True).min()

    high_points = recent_data[recent_data['high'] == rolling_high]
    low_points = recent_data[recent_data['low'] == rolling_low]

    high_dates = high_points['timestamp'].tolist()
    high_prices = high_points['high'].tolist()
    low_dates = low_points['timestamp'].tolist()
    low_prices = low_points['low'].tolist()

    return high_dates, high_prices, low_dates, low_prices

def check_trend(df, current_date, current_price, current_volume, prev_volume, trend_type):
    """
    현재 가격이 거래량 증가와 함께 하락추세선 상단을 돌파하거나 상승추세선 하단을 이탈하는지 판단.
    """
    high_dates, high_prices, low_dates, low_prices = get_highs_lows(df, trend_type)
    high_dates = [d.to_pydatetime() for d in high_dates]
    low_dates = [d.to_pydatetime() for d in low_dates]

    high_slope, high_intercept = get_trend_line(high_dates, high_prices)
    low_slope, low_intercept = get_trend_line(low_dates, low_prices)

    # 추세선 계산 불가 시 Normal 반환
    if high_slope is None or low_slope is None:
        return {"result": "Normal", "predicted_high": None, "predicted_low": None,
                "high_slope": None, "high_intercept": None,
                "low_slope": None, "low_intercept": None,
                "high_prices": max(high_prices) if high_prices else None,
                "low_prices": min(low_prices) if low_prices else None}

    predicted_high = predict_price(current_date, min(high_dates), high_slope, high_intercept)
    predicted_low = predict_price(current_date, min(low_dates), low_slope, low_intercept)

    # if current_price > predicted_high and current_volume > prev_volume:
    #     result = "Turn Up"
    #     return result
    # elif current_price < predicted_low and current_volume > prev_volume:
    #     result = "Turn Down"
    #     return result
    if current_price > predicted_high:
        result = "Turn Up"
    elif current_price < predicted_low:
        result = "Turn Down"
    else:
        result = "Normal"

    return {
        "result": result,
        "predicted_high": predicted_high,
        "predicted_low": predicted_low,
        "high_slope": high_slope,
        "high_intercept": high_intercept,
        "low_slope": low_slope,
        "low_intercept": low_intercept,
        "high_prices": max(high_prices) if high_prices else None,
        "low_prices": min(low_prices) if low_prices else None
    }

# 고점과 저점 계산 함수
def calculate_peaks_and_troughs(data):
    highs = []
    lows = []

    for i in range(1, len(data) - 1):
        prev_close = data['close'].iloc[i - 1]
        prev_low = data['low'].iloc[i - 1]
        prev_high = data['high'].iloc[i - 1]
        curr_close = data['close'].iloc[i]
        curr_low = data['low'].iloc[i]
        curr_high = data['high'].iloc[i]
        next_close = data['close'].iloc[i + 1]
        next_low = data['low'].iloc[i + 1]
        next_high = data['high'].iloc[i + 1]

        # 고점: 상승 후 하락
        if curr_high > prev_high and curr_high > next_high:
            highs.append(curr_high)
        else:
            highs.append(None)

        # 저점: 하락 후 상승
        if curr_low < prev_low and curr_low < next_low:
            lows.append(curr_low)
        else:
            lows.append(None)

    # 첫 번째와 마지막 값은 None 처리
    highs.insert(0, None)
    lows.insert(0, None)
    highs.append(None)
    lows.append(None)

    data['High Points'] = highs
    data['Low Points'] = lows
    return data

# 추세 판단 함수
def determine_trends(data):
    trend = []
    last_high = None
    last_low = None

    for i in range(len(data)):
        curr_close = data['close'].iloc[i]
        high_point = data['High Points'].iloc[i]
        low_point = data['Low Points'].iloc[i]

        if pd.notna(high_point):  # 고점 형성
            last_high = high_point

        if pd.notna(low_point):  # 저점 형성
            last_low = low_point

        # 상승 추세: 고점 재돌파
        if last_high and curr_close > last_high:
            trend.append('Uptrend')

        # 하락 추세: 저점 재이탈
        elif last_low and curr_close < last_low:
            trend.append('Downtrend')

        else:
            trend.append('Sideways')

    data['Trend'] = trend
    return data

# 이동평균선 및 거래량 급등 계산 함수
def calculate_indicators(data, timeframe):
    # MultiIndex를 단일 수준으로 변환
    # data.columns = ['_'.join(filter(None, col)) for col in data.columns]

    # 이동평균선 계산
    data['200MA'] = data['close'].rolling(window=200, min_periods=1).mean()

    # 타임프레임별 거래량 평균 윈도우 설정 (12시간 평균 기준)
    if timeframe == "4h":
        volume_window = 48  # 12시간 평균 (48 * 15분 = 12시간)
    elif timeframe == "1h":
        volume_window = 12  # 12시간 평균 (12 * 1시간 = 12시간)
    elif timeframe == "15m":
        volume_window = 48  # 12시간 평균 (48 * 15분 = 12시간)
    else:
        volume_window = 48  # 기본값

    # 12시간 거래량 평균 계산
    data['Volume Avg'] = data['volume'].rolling(window=volume_window, min_periods=1).mean()

    # NaN 값 처리 (NaN이 있는 행은 제외)
    data.dropna(subset=['volume', 'Volume Avg'], inplace=True)

    # 거래량 급등 여부 계산 (Volume > 1.5 * Volume Avg)
    data['Volume Surge'] = data['volume'] > (1.5 * data['Volume Avg'])

    return data

def update_tr_state(conn, state, signal_id, current_price=None, signal_price=None, prd_nm=None, tr_tp=None, market_kor_name=None, reentry_hours=16):
    with conn.cursor() as cur:

        if prd_nm:
            # tr_state = '02'인 데이터가 존재하는지 확인
            cur.execute(
                "SELECT id, chg_date FROM TR_SIGNAL_INFO WHERE prd_nm = %s AND tr_tp = %s AND tr_state = '02'",
                (prd_nm, tr_tp)
            )
            result = cur.fetchone()

            if result is None:

                query1 = """UPDATE TR_SIGNAL_INFO SET
                                tr_price = %s,
                                tr_dtm = %s,
                                tr_state = %s,
                                tr_count = 1,
                                chg_date = %s
                            WHERE id = %s
                        """
                cur.execute(query1, (current_price, datetime.now().strftime('%Y%m%d%H%M%S'), state,  datetime.now(), signal_id))

                if tr_tp == "B":
                    query2 = "UPDATE TR_SIGNAL_INFO SET tr_state = '24', u_tr_price = %s, chg_date = %s WHERE prd_nm = %s AND tr_tp = 'S' AND tr_state = '02'"
                    cur.execute(query2, (current_price, datetime.now(), prd_nm))

                    query3 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE prd_nm = %s AND tr_tp = 'S' AND tr_state = '01'"
                    cur.execute(query3, (datetime.now(), prd_nm))

                    query4 = "UPDATE TR_SIGNAL_INFO SET tr_state = '24', u_tr_price = %s, chg_date = %s WHERE prd_nm = %s AND tr_tp = 'B' AND tr_state = '22'"
                    cur.execute(query4, (current_price, datetime.now(), prd_nm))
                else:
                    query2 = "UPDATE TR_SIGNAL_INFO SET tr_state = '24', u_tr_price = %s, chg_date = %s WHERE prd_nm = %s AND tr_tp = 'B' AND tr_state = '02'"
                    cur.execute(query2, (current_price, datetime.now(), prd_nm))

                    query3 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE prd_nm = %s AND tr_tp = 'B' AND tr_state = '01'"
                    cur.execute(query3, (datetime.now(), prd_nm))

                    query4 = "UPDATE TR_SIGNAL_INFO SET tr_state = '24', u_tr_price = %s, chg_date = %s WHERE prd_nm = %s AND tr_tp = 'S' AND tr_state = '21'"
                    cur.execute(query4, (current_price, datetime.now(), prd_nm))

                conn.commit()

                return "new"

            else:
                formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                existing_id = result[0]
                last_dtm = result[1]
                pre_sixteen_hour_dtm = datetime.now() - timedelta(hours=reentry_hours)

                if last_dtm < pre_sixteen_hour_dtm:

                    # 추가 매매하기 위해 기존 매매정보 기준 신규 매매정보 생성 및 기존 매매정보 변경 처리(tr_state ='23')
                    if tr_tp == "B":

                        # INSERT 실행 후 ID 가져오기
                        query_insert = """
                            INSERT INTO TR_SIGNAL_INFO (
                                prd_nm, tr_tp, tr_dtm, tr_state, tr_price, signal_name, regr_id, reg_date, chgr_id, chg_date, support_price, regist_price, tr_count
                            )
                            SELECT
                                prd_nm, tr_tp, %s, '02', %s, signal_name, 'AUTO_SIGNAL', %s, 'AUTO_SIGNAL', %s,
                                CASE WHEN %s > tr_price THEN tr_price ELSE support_price END, %s, tr_count + 1
                            FROM TR_SIGNAL_INFO
                            WHERE id = %s
                            RETURNING id
                        """

                        cur.execute(query_insert, (
                            datetime.now().strftime('%Y%m%d%H%M%S'),  # tr_dtm
                            current_price,  # tr_price
                            datetime.now(),  # reg_date
                            datetime.now(),  # chg_date
                            current_price,  # 비교할 현재 가격
                            signal_price,  # 등록 가격
                            existing_id  # 기존 신호 ID
                        ))

                        result = cur.fetchone()  # 새로 삽입된 ID 가져오기
                        if result:
                            new_signal_id = result[0]  # 반환된 id 저장

                            # UPDATE 실행
                            query_update = """
                                UPDATE TR_SIGNAL_INFO
                                SET tr_state = '23',
                                    u_tr_price = %s,
                                    chg_date = %s
                                WHERE id = %s
                            """
                            cur.execute(query_update, (current_price, datetime.now(), existing_id))

                            # 다른 신호 상태 변경
                            query3 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE prd_nm = %s AND tr_tp = 'S' AND tr_state = '01'"
                            cur.execute(query3, (datetime.now(), prd_nm))

                            conn.commit()

                            message = f"{market_kor_name}[{prd_nm}] 추가 매수 신호 발생 시간: {formatted_datetime}, 현재가: {current_price} "
                            print(message)
                            send_slack_message("#매매신호", message)

                    else:

                        # 기존 신호 '02' 상태가 있는지 확인
                        query_check = "SELECT 1 FROM TR_SIGNAL_INFO WHERE tr_tp = 'B' AND prd_nm = %s AND tr_state = '02' LIMIT 1"
                        cur.execute(query_check, (prd_nm,))
                        exists = cur.fetchone()

                        if exists:  # 기존 신호가 존재할 때만 실행
                            # INSERT 실행 후 ID 가져오기
                            query_insert = """
                                INSERT INTO TR_SIGNAL_INFO (
                                    prd_nm, tr_tp, tr_dtm, tr_state, tr_price, signal_name, regr_id, reg_date, chgr_id, chg_date, support_price, regist_price, tr_count
                                )
                                SELECT
                                    prd_nm, tr_tp, %s, '02', %s, signal_name, 'AUTO_SIGNAL', %s, 'AUTO_SIGNAL', %s, %s,
                                    CASE WHEN %s < tr_price THEN tr_price ELSE regist_price END, tr_count + 1
                                FROM TR_SIGNAL_INFO
                                WHERE id = %s
                                RETURNING id
                            """

                            cur.execute(query_insert, (
                                datetime.now().strftime('%Y%m%d%H%M%S'),  # tr_dtm
                                current_price,  # tr_price
                                datetime.now(),  # reg_date
                                datetime.now(),  # chg_date
                                signal_price,  # support_price
                                current_price,  # regist_price
                                existing_id  # 기존 신호 ID
                            ))

                            result = cur.fetchone()
                            if result:
                                new_signal_id = result[0]  # 반환된 ID 저장

                                # UPDATE 실행
                                query_update = """
                                    UPDATE TR_SIGNAL_INFO
                                    SET tr_state = '23',
                                        u_tr_price = %s,
                                        chg_date = %s
                                    WHERE id = %s
                                    RETURNING TRUE
                                """
                                cur.execute(query_update, (current_price, datetime.now(), existing_id))
                                update_result = cur.fetchone()

                                if update_result:
                                    # 추가 업데이트 실행
                                    query3 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE prd_nm = %s AND tr_tp = 'B' AND tr_state = '01'"
                                    cur.execute(query3, (datetime.now(), prd_nm))

                                    conn.commit()

                                    formatted_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    message = f"{market_kor_name}[{prd_nm}] 추가 매도 신호 발생 시간: {formatted_datetime}, 현재가: {current_price} "
                                    print(message)
                                    send_slack_message("#매매신호", message)

                    return "exists"

        else:

            query = """UPDATE TR_SIGNAL_INFO SET
                                tr_state = %s,
                                chg_date = %s
                            WHERE id = %s
                        """
            cur.execute(query, (state, datetime.now(), signal_id))
            conn.commit()

            return "update"

# Slack 메시지 전송 함수
def send_slack_message(channel, message):
    try:
        if message not in sent_messages:  # 이전에 전송되지 않은 메시지만 전송
            response = client.chat_postMessage(channel=channel, text=message)
            print(f"Slack 메시지 전송 성공: {response['message']['text']}")
            sent_messages.add(message)  # 메시지를 기록
        # else:
        #     print("중복 메시지, 전송 생략:", message)
    except SlackApiError as e:
        print(f"Slack 메시지 전송 실패: {e.response['error']}")

# 데이터 가져오기 재시도 함수(재시도 횟수 : 3회)
def fetch_ohlcv_with_retry(exchange, symbol, timeframe_15m, limit=200, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            return exchange.fetch_ohlcv(symbol, timeframe=timeframe_15m, limit=limit)
        except Exception as e:
            print(f"Error fetching OHLCV data: {e}")
            retries += 1
            time.sleep(1)  # 재시도 전 1초 대기

    print("Max retries reached. Failed to fetch OHLCV data.")
    return None  # 실패 시 None 반환

def calculate_atr_pct(exchange, symbol, period=14):
    """일봉 데이터로 ATR% 계산 (ATR / 현재가 * 100)"""
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=period + 1)
        if ohlcv is None or len(ohlcv) < period + 1:
            return None

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # True Range 계산
        df['prev_close'] = df['close'].shift(1)
        df['tr'] = df.apply(lambda row: max(
            row['high'] - row['low'],
            abs(row['high'] - row['prev_close']) if pd.notna(row['prev_close']) else 0,
            abs(row['low'] - row['prev_close']) if pd.notna(row['prev_close']) else 0
        ), axis=1)

        atr = df['tr'].iloc[1:].mean()  # 첫 행 제외 (prev_close 없음)
        current_price = df['close'].iloc[-1]

        if current_price == 0:
            return None

        atr_pct = (atr / current_price) * 100
        return {'atr_pct': round(float(atr_pct), 4), 'atr_value': float(atr), 'current_price': float(current_price)}

    except Exception as e:
        print(f"ATR% 계산 실패 [{symbol}]: {e}")
        return None

def classify_trend_type(atr_pct):
    """ATR% 임계값으로 trend_type 분류"""
    if atr_pct >= ATR_THRESHOLD_HIGH:
        return 'short'
    elif atr_pct >= ATR_THRESHOLD_MID:
        return 'mid'
    elif atr_pct >= ATR_THRESHOLD_LOW:
        return 'long'
    else:
        return 'watch'

def assess_daily_volatility():
    """상위 거래량 종목의 ATR% 계산 → trend_type 분류 → DB 저장 → Slack 알림"""
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.now(timezone).date()

    refresh_top_volume_markets()
    top_markets = get_top_volume_markets()

    market_trend_map = {}
    slack_lines = ["[일일 변동성 평가 결과]", f"기준일: {today}", ""]

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

    try:
        with conn.cursor() as cur:
            for market_currency, korean_name in top_markets:
                atr_result = calculate_atr_pct(exchange, market_currency)
                time.sleep(0.2)  # API rate limit

                if atr_result is None:
                    print(f"{korean_name}[{market_currency}] ATR% 계산 실패 - 기본값(long) 적용")
                    trend_type = 'long'
                    atr_pct = 0.0
                    atr_value = 0.0
                    current_price = 0.0
                else:
                    atr_pct = atr_result['atr_pct']
                    atr_value = atr_result['atr_value']
                    current_price = atr_result['current_price']
                    trend_type = classify_trend_type(atr_pct)

                market_trend_map[market_currency] = {
                    'trend_type': trend_type,
                    'korean_name': korean_name
                }

                cur.execute("""
                    INSERT INTO MARKET_VOLATILITY (base_date, market_code, trend_type, atr_pct, atr_value, current_price)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (base_date, market_code)
                    DO UPDATE SET
                        trend_type = EXCLUDED.trend_type,
                        atr_pct = EXCLUDED.atr_pct,
                        atr_value = EXCLUDED.atr_value,
                        current_price = EXCLUDED.current_price,
                        reg_date = NOW()
                """, (today, market_currency, trend_type, atr_pct, atr_value, current_price))

                type_label = {'short': '단기(고변동)', 'mid': '중기(중변동)', 'long': '장기(저변동)', 'watch': '관망(극저변동)'}
                slack_lines.append(f"  {korean_name}[{market_currency}] ATR%: {atr_pct:.2f}% → {type_label.get(trend_type, trend_type)}")

            conn.commit()

    except Exception as e:
        print(f"변동성 평가 오류: {e}")
        conn.rollback()
    finally:
        conn.close()

    slack_message = "\n".join(slack_lines)
    print(slack_message)
    send_slack_message("#매매신호", slack_message)

    return market_trend_map

def get_daily_volatility():
    """DB에서 당일 변동성 데이터 조회, 없으면 assess_daily_volatility() 호출"""
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.now(timezone).date()

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT v.market_code, v.trend_type, t.korean_name
                FROM MARKET_VOLATILITY v
                JOIN MARKET_TOP_VOLUME t ON v.market_code = t.market_currency AND v.base_date = t.base_date
                WHERE v.base_date = %s
                ORDER BY t.ranking
            """, (today,))
            results = cur.fetchall()
    finally:
        conn.close()

    if not results:
        return assess_daily_volatility()

    market_trend_map = {}
    for market_code, trend_type, korean_name in results:
        market_trend_map[market_code] = {
            'trend_type': trend_type,
            'korean_name': korean_name
        }

    return market_trend_map

def create_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MARKET_TOP_VOLUME (
                id SERIAL PRIMARY KEY,
                base_date DATE NOT NULL,
                market_code VARCHAR(20) NOT NULL,
                market_currency VARCHAR(20) NOT NULL,
                korean_name VARCHAR(50),
                acc_trade_price NUMERIC(30, 2),
                ranking INTEGER NOT NULL,
                reg_date TIMESTAMP DEFAULT NOW(),
                UNIQUE(base_date, market_code)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MARKET_VOLATILITY (
                id SERIAL PRIMARY KEY,
                base_date DATE NOT NULL,
                market_code VARCHAR(20) NOT NULL,
                trend_type VARCHAR(10) NOT NULL,
                atr_pct NUMERIC(10, 4),
                atr_value NUMERIC(30, 8),
                current_price NUMERIC(30, 8),
                reg_date TIMESTAMP DEFAULT NOW(),
                UNIQUE(base_date, market_code)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TR_SIGNAL_INFO (
                id SERIAL PRIMARY KEY,
                prd_nm VARCHAR(20),
                tr_tp VARCHAR(2),
                tr_dtm VARCHAR(14),
                tr_state VARCHAR(2),
                tr_price NUMERIC(30, 8),
                tr_volume NUMERIC(30, 8),
                u_tr_price NUMERIC(30, 8),
                signal_name VARCHAR(50),
                support_price NUMERIC(30, 8),
                regist_price NUMERIC(30, 8),
                tr_count INTEGER DEFAULT 0,
                regr_id VARCHAR(20),
                reg_date TIMESTAMP DEFAULT NOW(),
                chgr_id VARCHAR(20),
                chg_date TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
    conn.close()

def refresh_top_volume_markets():
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.now(timezone).date()

    url = "https://api.upbit.com/v1/market/all?is_details=false"
    headers = {"accept": "application/json"}
    market_list = requests.get(url, headers=headers).json()

    krw_markets = []
    name_map = {}
    for item in market_list:
        market_str = item.get('market', '')
        korean_name = item.get('korean_name', '')
        if market_str.startswith('KRW-'):
            krw_markets.append(market_str)
            name_map[market_str] = korean_name

    markets_param = ','.join(krw_markets)
    ticker_url = f"https://api.upbit.com/v1/ticker?markets={markets_param}"
    ticker_data = requests.get(ticker_url, headers=headers).json()

    ticker_data.sort(key=lambda x: x.get('acc_trade_price_24h', 0), reverse=True)
    top10 = ticker_data[:10]

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    with conn.cursor() as cur:
        for rank, item in enumerate(top10, 1):
            market_code = item['market']
            currency, market = market_code.split('-')
            market_currency = f"{market}/{currency}"
            korean_name = name_map.get(market_code, '')
            acc_trade_price = item.get('acc_trade_price_24h', 0)
            cur.execute("""
                INSERT INTO MARKET_TOP_VOLUME (base_date, market_code, market_currency, korean_name, acc_trade_price, ranking)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (base_date, market_code)
                DO UPDATE SET
                    market_currency = EXCLUDED.market_currency,
                    korean_name = EXCLUDED.korean_name,
                    acc_trade_price = EXCLUDED.acc_trade_price,
                    ranking = EXCLUDED.ranking,
                    reg_date = NOW()
            """, (today, market_code, market_currency, korean_name, acc_trade_price, rank))
        conn.commit()
    conn.close()
    print(f"거래대금 상위 10개 종목 DB 저장 완료 (기준일: {today})")

def get_top_volume_markets():
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.now(timezone).date()

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT market_currency, korean_name FROM MARKET_TOP_VOLUME WHERE base_date = %s ORDER BY ranking",
            (today,)
        )
        results = cur.fetchall()
    conn.close()

    if not results:
        refresh_top_volume_markets()
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT market_currency, korean_name FROM MARKET_TOP_VOLUME WHERE base_date = %s ORDER BY ranking",
                (today,)
            )
            results = cur.fetchall()
        conn.close()

    return results

def analyze_data(trend_type, target_market=None):
    # trend_type에 따른 타임프레임 선택
    if trend_type.lower() == 'long':
        timeframe = "4h"
        lookback_hours = 16  # 최근 캔들 필터링 기준
        trend_label = "장기"
    elif trend_type.lower() == 'short':
        timeframe = "15m"
        lookback_hours = 4   # 15분봉은 더 짧은 기간
        trend_label = "단기"
    else:  # 'mid' 또는 기타
        timeframe = "1h"
        lookback_hours = 8   # 1시간봉은 중간 기간
        trend_label = "중기"

    timezone = pytz.timezone('Asia/Seoul')
    end_time = datetime.now(timezone)
    lookback_time = end_time - timedelta(hours=lookback_hours)

    if target_market:
        top_markets = [target_market]  # (market_currency, korean_name) 튜플
    else:
        top_markets = get_top_volume_markets()

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    try:
        count = 0
        for market_currency, market_kor_name in top_markets:
            count += 1

            # 동적 타임프레임으로 데이터 가져오기
            ohlcv = fetch_ohlcv_with_retry(exchange, market_currency, timeframe)

            if ohlcv is None or len(ohlcv) < 200:
                print(f"{market_kor_name}[{market_currency}] {trend_label} 추세라인 미처리 => {end_time}")
                continue

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 고점/저점 계산, 이동평균선 및 거래량 급등 계산
            df = calculate_peaks_and_troughs(df)
            df = calculate_indicators(df, timeframe)

            # 불변값 1회 계산
            current_date = df.iloc[-1]['timestamp']
            current_price = float(df.iloc[-1]['close'])
            current_volume = float(df.iloc[-1]['volume'])
            prev_volume = float(df.iloc[-2]['volume'])

            # check_trend 1회 호출
            trend_info = check_trend(df, current_date, current_price, current_volume, prev_volume, trend_type)

            # Phase 1: 매수 신호 체크/발동
            signal_buy = "01"

            with conn.cursor() as cur01:
                query1 = "SELECT id, tr_dtm, tr_price, tr_volume, chg_date FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'B' AND tr_state = '01' order by tr_dtm desc"
                cur01.execute(query1, (market_currency, ))
                result_01 = cur01.fetchall()

            if result_01:
                for idx, result in enumerate(result_01):
                    if idx == 0 and float(df['close'].iloc[-1]) >= result[2] and float(df['volume'].iloc[-1]) > result[3] and signal_buy == "01":
                        formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{market_kor_name}[{market_currency}] 매수 신호 발생 시간: {formatted_datetime}, 현재가: {df['close'].iloc[-1]} 하락추세선 상단 돌파한 고점 {round(result[2], 1)} 을 돌파하였습니다."
                        print(message)

                        result = update_tr_state(conn, '02', result[0], float(df['close'].iloc[-1]), result[2], market_currency, 'B', market_kor_name, reentry_hours=REENTRY_HOURS.get(trend_type, 16))

                        if result == "new":
                            signal_buy = "02"
                            send_slack_message("#매매신호", message)
                        elif result == "exists":
                            signal_buy = "02"

                    elif signal_buy == "02":
                        update_tr_state(conn, '11', result[0])

            # Phase 2: 매도 신호 체크/발동
            signal_sell = "01"

            with conn.cursor() as cur02:
                query2 = "SELECT id, tr_dtm, tr_price, tr_volume, chg_date FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '01' order by tr_dtm desc"
                cur02.execute(query2, (market_currency, ))
                result_02 = cur02.fetchall()

            if result_02:
                for idx, result in enumerate(result_02):
                    if idx == 0 and float(df['close'].iloc[-1]) <= result[2] and float(df['volume'].iloc[-1]) > result[3] and signal_sell == "01":
                        formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{market_kor_name}[{market_currency}] 매도 신호 발생 시간: {formatted_datetime}, 현재가: {df['close'].iloc[-1]} 상승추세선 하단 이탈한 저점 {round(result[2], 1)} 을 이탈하였습니다."
                        print(message)

                        result = update_tr_state(conn, '02', result[0], float(df['close'].iloc[-1]), result[2], market_currency, 'S', market_kor_name, reentry_hours=REENTRY_HOURS.get(trend_type, 16))

                        if result == "new":
                            signal_sell = "02"
                            send_slack_message("#매매신호", message)
                        elif result == "exists":
                            signal_sell = "02"

                    elif signal_sell == "02":
                        update_tr_state(conn, '11', result[0])

            # Phase 3: 신호 생성 (최근 캔들만 필터링)
            print(f"{market_kor_name}[{market_currency}] {trend_label} 추세라인 분석 종료 시간: {end_time}")

            recent_candles = df[df['timestamp'] >= lookback_time]

            for _, row_15m in recent_candles.iterrows():
                timestamp = row_15m['timestamp']
                h_close = row_15m['high']
                volume_surge = row_15m['Volume Surge']
                volume = row_15m['volume']

                if trend_info['result'] == "Turn Up" and volume_surge:

                    with conn.cursor() as cur:
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')

                        # 매매신호정보 존재여부 조회
                        query01 = "SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'B' AND tr_dtm = %s"
                        cur.execute(query01, (market_currency, tr_dtm))
                        result_one = cur.fetchone()

                        if result_one is None:
                            insert_query = """
                                INSERT INTO TR_SIGNAL_INFO (
                                    prd_nm, tr_tp, tr_dtm, tr_state, tr_price, tr_volume, signal_name,
                                    regr_id, reg_date, chgr_id, chg_date, support_price, regist_price
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cur.execute(insert_query, (
                                market_currency, "B", tr_dtm, "01", h_close, volume, f"TrendLine-{trend_type}",
                                "AUTO_SIGNAL", datetime.now(), "AUTO_SIGNAL", datetime.now(),
                                trend_info["low_prices"], trend_info["high_prices"]
                            ))
                            conn.commit()

                        # 매매신호정보 매도 '02' 상태의 대상 조회
                        query02 = """
                            SELECT
                                id,
                                regist_price
                            FROM TR_SIGNAL_INFO
                            WHERE signal_name = %s
                            AND prd_nm = %s
                            AND tr_tp = 'S'
                            AND tr_state = '02'
                        """
                        cur.execute(query02, (f"TrendLine-{trend_type}", market_currency))
                        results = cur.fetchall()

                        regist_price = None

                        for row in results:
                            existing_id = row[0]
                            regist_price = row[1]

                            # 고가가 저항가격보다 큰 경우 업데이트(tr_state = '21')
                            if float(regist_price) < trend_info["high_prices"]:
                                update_query1 = "UPDATE TR_SIGNAL_INFO SET tr_state = '21', u_tr_price = %s, chg_date = %s WHERE id = %s"
                                cur.execute(update_query1, (float(current_price), datetime.now(), existing_id))

                                update_query2 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE signal_name = %s AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '01'"
                                cur.execute(update_query2, (datetime.now(), f"TrendLine-{trend_type}", market_currency))

                                conn.commit()

                                formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                message = f"{market_kor_name}[{market_currency}] 매도 추세 마감 신호 발생 시간: {formatted_datetime}, 현재가: {current_price} "
                                print(message)
                                send_slack_message("#매매신호", message)

                elif trend_info['result'] == "Turn Down" and volume_surge:

                    with conn.cursor() as cur:
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')

                        # 매매신호정보 존재여부 조회
                        query01 = "SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_dtm = %s"
                        cur.execute(query01, (market_currency, tr_dtm))
                        result_one = cur.fetchone()

                        if result_one is None:
                            insert_query = """
                                INSERT INTO TR_SIGNAL_INFO (
                                    prd_nm, tr_tp, tr_dtm, tr_state, tr_price, tr_volume, signal_name,
                                    regr_id, reg_date, chgr_id, chg_date, support_price, regist_price
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cur.execute(insert_query, (
                                market_currency, "S", tr_dtm, "01", h_close, volume, f"TrendLine-{trend_type}",
                                "AUTO_SIGNAL", datetime.now(), "AUTO_SIGNAL", datetime.now(),
                                trend_info["low_prices"], trend_info["high_prices"]
                            ))
                            conn.commit()

                        # 매매신호정보 매수 '02' 상태의 대상 조회
                        query02 = """
                            SELECT
                                id,
                                support_price
                            FROM TR_SIGNAL_INFO
                            WHERE signal_name = %s
                            AND prd_nm = %s
                            AND tr_tp = 'B'
                            AND tr_state = '02'
                        """
                        cur.execute(query02, (f"TrendLine-{trend_type}", market_currency))
                        results = cur.fetchall()

                        support_price = None

                        for row in results:
                            existing_id = row[0]
                            support_price = row[1]

                            # 지지가격보다 저가가 작은 경우 업데이트(tr_state = '22')
                            if float(support_price) > trend_info["low_prices"]:
                                update_query1 = "UPDATE TR_SIGNAL_INFO SET tr_state = '22', u_tr_price = %s, chg_date = %s WHERE id = %s"
                                cur.execute(update_query1, (float(current_price), datetime.now(), existing_id))

                                update_query2 = "UPDATE TR_SIGNAL_INFO SET tr_state = '11', chg_date = %s WHERE signal_name = %s AND prd_nm = %s AND tr_tp = 'B' AND tr_state = '01'"
                                cur.execute(update_query2, (datetime.now(), f"TrendLine-{trend_type}", market_currency))

                                conn.commit()

                                formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                message = f"{market_kor_name}[{market_currency}] 매수 추세 마감 신호 발생 시간: {formatted_datetime}, 현재가: {current_price} "
                                print(message)
                                send_slack_message("#매매신호", message)

            print(f"count : {count}")

    except Exception as e:
        print("에러 발생:", e)
    finally:
        conn.close()

def run_volatility_analysis():
    """변동성 기반 자동 추세 분석 실행"""
    market_trend_map = get_daily_volatility()

    if not market_trend_map:
        print("변동성 평가 결과 없음. 기본값(long)으로 실행.")
        analyze_data('long')
        return

    for market_currency, info in market_trend_map.items():
        trend_type = info['trend_type']
        korean_name = info['korean_name']

        if trend_type == 'watch':
            print(f"{korean_name}[{market_currency}] 관망 - 분석 건너뜀")
            continue

        analyze_data(trend_type, target_market=(market_currency, korean_name))
        time.sleep(1)  # API rate limit

def daily_volatility_refresh():
    """9시 변동성 평가 갱신"""
    assess_daily_volatility()

if __name__ == "__main__":
    # create_tables()
    schedule.every().day.at("09:00").do(daily_volatility_refresh)
    # schedule.every(1).minutes.do(run_volatility_analysis)

    print("변동성 기반 자동 추세 분석 시작...")
    run_volatility_analysis()  # 첫 실행
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
