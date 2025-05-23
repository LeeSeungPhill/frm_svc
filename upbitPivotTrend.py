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
import psycopg2

# 업비트 API 키 설정
API_KEY = os.environ['UPBIT_ACCESS_KEY']
SECRET_KEY = os.environ['UPBIT_SECRET_KEY']

# 업비트 거래소 초기화
exchange = ccxt.upbit({
    'apiKey': API_KEY,
    'secret': SECRET_KEY
})

# Slack 메세지 연동
SLACK_TOKEN = "xoxb-" + "8297506351525-" + "8285909742855-" + "0dYYONPibZwn0JdlWWAKVc6g"
client = slack_sdk.WebClient(token=SLACK_TOKEN)

# 전송된 메시지 기록 저장 (전역 변수)
sent_messages = set()

# 데이터베이스 연결 정보
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"  # 원격 서버라면 해당 서버의 IP 또는 도메인
DB_PORT = "5432"  # 기본 포트

# 피봇 포인트 계산 함수
def calculate_pivot_points(df):
    high = df['high'].iloc[-1]
    low = df['low'].iloc[-1]
    close = df['close'].iloc[-1]

    pivot = (high + low + close) / 3
    resistance1 = 2 * pivot - low
    support1 = 2 * pivot - high

    return pivot, resistance1, support1

# RSI 계산 함수
def calculate_rsi(df, period=14):
    delta = df['close'].diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 고점과 저점 계산 함수
def calculate_peaks_and_troughs(data):
    highs = []
    lows = []

    for i in range(1, len(data) - 1):
        prev_close = data['close'].iloc[i - 1]
        curr_close = data['close'].iloc[i]
        next_close = data['close'].iloc[i + 1]

        # 고점: 상승 후 하락
        if curr_close > prev_close and curr_close > next_close:
            highs.append(curr_close)
        else:
            highs.append(None)

        # 저점: 하락 후 상승
        if curr_close < prev_close and curr_close < next_close:
            lows.append(curr_close)
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
def calculate_indicators(data):
    # MultiIndex를 단일 수준으로 변환
    # data.columns = ['_'.join(filter(None, col)) for col in data.columns]

    # 이동평균선 계산
    data['200MA'] = data['close'].rolling(window=200, min_periods=1).mean()
    
    # 12시간 거래량 평균 계산
    data['Volume Avg'] = data['volume'].rolling(window=48, min_periods=1).mean()

    # NaN 값 처리 (NaN이 있는 행은 제외)
    data.dropna(subset=['volume', 'Volume Avg'], inplace=True)

    # 거래량 급등 여부 계산 (Volume > 1.5 * Volume Avg)
    data['Volume Surge'] = data['volume'] > (1.5 * data['Volume Avg'])

    return data

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

def analyze_data():
    # 감시할 코인
    params = ["BTC/KRW","XRP/KRW","ETH/KRW","ONDO/KRW","STX/KRW","SOL/KRW","SUI/KRW","XLM/KRW","HBAR/KRW","ADA/KRW","LINK/KRW","RENDER/KRW"]
    timeframe = "15m"  # 15분봉 데이터 사용
    end_time = datetime.now(pytz.timezone('Asia/Seoul'))

    try:

        for i in params:
            # 시세 데이터 가져오기
            ohlcv = exchange.fetch_ohlcv(i, timeframe=timeframe, limit=200)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 고점/저점 계산, 추세 판단, 이동평균선 및 거래량 급등 계산
            df = calculate_peaks_and_troughs(df)
            df = determine_trends(df)
            df = calculate_indicators(df)

            # 피봇 포인트와 RSI 계산
            pivot, resistance1, support1 = calculate_pivot_points(df)
            df['rsi'] = calculate_rsi(df)

            # 결과 출력
            print(f"{i} 분석 종료 시간: {end_time}")

            one_hour_ago = end_time - timedelta(hours=1)

            for _, row in df.iterrows():
                timestamp = row['timestamp']
                close = row['close']
                rsi = row['rsi']
                trend = row['Trend']
                volume_surge = row['Volume Surge']
                volume = row['volume']
                ma_200 = row['200MA']

                # if volume_surge and close > ma_200:
                #     print(f"거래량 급등하며 200일 이동평균선 돌파 신호 발생 시간: {timestamp}")
                # elif volume_surge and close < ma_200:
                #     print(f"거래량 급등하며 200일 이동평균선 이탈 신호 발생 시간: {timestamp}")
                
                if timestamp >= one_hour_ago:
                    # 매수 조건
                    if volume_surge and trend == 'Uptrend':
                        # print(f"상승 추세 (고점 재돌파) 신호 발생 시간: {timestamp}")
                        if close <= support1 and close < ma_200:
                        # if rsi < 30 and close <= support1:
                            message = f"{i} 매수 신호 발생 시간: {timestamp}, 가격: {close}, RSI: {rsi}, Trend: {trend}"
                            # print(message)
                            # client.chat_postMessage(channel='#가상화폐-자동매매',text= message)
                            
                            # PostgreSQL 데이터베이스에 연결
                            conn = psycopg2.connect(
                                dbname=DB_NAME,
                                user=DB_USER,
                                password=DB_PASSWORD,
                                host=DB_HOST,
                                port=DB_PORT
                            )
                            
                            # 커서 생성
                            cur1 = conn.cursor()
                            
                            tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                            
                            # 매매신호정보 존재여부 조회
                            cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE prd_nm = '"+i+"' AND tr_tp = 'B' AND tr_dtm = '"+tr_dtm+"'")
                            result_one = cur1.fetchone()
                            print("result_one :", result_one)
                            
                            if result_one is None:
                                cur2 = conn.cursor()
                                ins_param1 = (
                                    i,               # prd_nm
                                    "B",             # tr_tp
                                    tr_dtm,          # tr_dtm
                                    "01",            # tr_state
                                    close,           # tr_price
                                    volume,          # tr_volume
                                    "PivotTrend",    # signal_name
                                    "AUTO_SIGNAL",   # regr_id
                                    datetime.now(),  # reg_date
                                    "AUTO_SIGNAL",   # chgr_id
                                    datetime.now()   # chg_date
                                )
                                
                                insert1 = """INSERT INTO TR_SIGNAL_INFO (
                                                prd_nm,
                                                tr_tp,
                                                tr_dtm,
                                                tr_state,
                                                tr_price,
                                                tr_volume,
                                                signal_name,
                                                regr_id,
                                                reg_date,
                                                chgr_id,
                                                chg_date
                                            ) VALUES (
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                            )"""
                                cur2.execute(insert1, ins_param1)
                                conn.commit()
                                cur2.close()
                                
                                # Slack 메시지 전송
                                send_slack_message("#매매신호", message)
                            
                            # 연결 종료
                            cur1.close()
                            conn.close()
                            print("PostgreSQL 연결 종료")
                                                      
                    # 매도 조건
                    elif volume_surge and trend == 'Downtrend':
                        # print(f"하락 추세 (저점 재이탈) 신호 발생 시간: {timestamp}")
                        # if rsi > 70 and close >= resistance1:
                        if close >= resistance1 and close > ma_200:
                            message = f"{i} 매도 신호 발생 시간: {timestamp}, 가격: {close}, RSI: {rsi}, Trend: {trend}"
                            # print(message)
                            # client.chat_postMessage(channel='#가상화폐-자동매매',text= message)
                            
                            # PostgreSQL 데이터베이스에 연결
                            conn = psycopg2.connect(
                                dbname=DB_NAME,
                                user=DB_USER,
                                password=DB_PASSWORD,
                                host=DB_HOST,
                                port=DB_PORT
                            )
                            
                            # 커서 생성
                            cur1 = conn.cursor()
                            
                            tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                            
                            # 매매신호정보 존재여부 조회
                            cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE prd_nm = '"+i+"' AND tr_tp = 'S' AND tr_dtm = '"+tr_dtm+"'")
                            result_one = cur1.fetchone()
                            print("result_one :", result_one)
                            
                            if result_one is None:
                                cur2 = conn.cursor()
                                ins_param1 = (
                                    i,               # prd_nm
                                    "S",             # tr_tp
                                    tr_dtm,          # tr_dtm
                                    "01",            # tr_state
                                    close,           # tr_price
                                    volume,          # tr_volume
                                    "PivotTrend",    # signal_name
                                    "AUTO_SIGNAL",   # regr_id
                                    datetime.now(),  # reg_date
                                    "AUTO_SIGNAL",   # chgr_id
                                    datetime.now()   # chg_date
                                )
                                
                                insert1 = """INSERT INTO TR_SIGNAL_INFO (
                                                prd_nm,
                                                tr_tp,
                                                tr_dtm,
                                                tr_state,
                                                tr_price,
                                                tr_volume,
                                                signal_name,
                                                regr_id,
                                                reg_date,
                                                chgr_id,
                                                chg_date
                                            ) VALUES (
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                            )"""
                                cur2.execute(insert1, ins_param1)
                                conn.commit()
                                cur2.close()
                                
                                # Slack 메시지 전송
                                send_slack_message("#매매신호", message)
                            
                            # 연결 종료
                            cur1.close()
                            conn.close()
                            print("PostgreSQL 연결 종료")

    except Exception as e:
        print("에러 발생:", e)

# 15분마다 실행 설정
schedule.every(15).minutes.do(analyze_data)        

# 실행
if __name__ == "__main__":
    print("15분마다 분석 작업을 실행합니다...")
    analyze_data()  # 첫 실행
    while True:
        schedule.run_pending()
        time.sleep(1)