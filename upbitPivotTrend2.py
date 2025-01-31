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

# 피봇 포인트 계산 함수
def calculate_pivot_points(df):
    high = df['high'].iloc[-1]
    low = df['low'].iloc[-1]
    close = df['close'].iloc[-1]

    pivot = (high + low + close) / 3
    resistance1 = 2 * pivot - low
    support1 = 2 * pivot - high
    resistance2 = pivot + high - low
    support2 = pivot - high + low

    return pivot, resistance1, support1, resistance2, support2

# 피보나치 계산 함수
def calculate_fibonacci(df):
    high = df['high'].iloc[-1]
    low = df['low'].iloc[-1]
    close = df['close'].iloc[-1]

    f_pivot = (high + low + close) / 3
    f_resistance1 = f_pivot + 0.382 * (high- low)
    f_support1 = f_pivot - 0.382 * (high - low)
    f_resistance2 = f_pivot + 0.618 * (high - low)
    f_support2 = f_pivot - 0.618 * (high - low)
    f_resistance3= f_pivot + 1 * (high - low)
    f_support3 = f_pivot - 1 * (high - low)

    return f_pivot, f_resistance1, f_support1, f_resistance2, f_support2, f_resistance3, f_support3

# RSI 계산 함수
def calculate_rsi(df, period=14):
    if len(df) < period:
        return None  # 데이터 부족 시 None 반환

    # 종가 데이터 가져오기
    prices = df['close']

    # 종가 변화량 계산
    delta = prices.diff().dropna()

    # 상승분과 하락분 분리
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)

    # 초기 평균 상승 및 평균 하락 계산
    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])

    # 이후 값들은 지수 이동 평균 방식으로 계산
    for i in range(period, len(delta)):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period

    # 현재 봉 기준 RSI 계산
    if avg_loss == 0:
        return 100  # 손실이 없으면 RSI는 100
    else:
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

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

# 추세 판단 및 피봇 포인트 계산 함수
def determine_trends(data):
    resistance1 = []
    resistance2 = []
    support1 = []
    support2 = []
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

    high = data['high'].rolling(window=48, min_periods=1).mean()
    low = data['low'].rolling(window=48, min_periods=1).mean()

    # 피봇 포인트 계산
    pivot = (high + low + curr_close) / 3
    resistance1 = 2 * pivot - low
    support1 = 2 * pivot - high
    resistance2 = pivot + high - low
    support2 = pivot - high + low
    
    data['Trend'] = trend
    data['resistance1'] = resistance1
    data['support1'] = support1
    data['resistance2'] = resistance2
    data['support2'] = support2
    return data

# 이동평균선 및 거래량 급등 계산 함수
def calculate_indicators(data):
    # MultiIndex를 단일 수준으로 변환
    # data.columns = ['_'.join(filter(None, col)) for col in data.columns]

    # 200일 이동평균선 계산
    data['200MA'] = data['close'].rolling(window=200, min_periods=1).mean()
    
    # 20일 거래량 평균 계산
    data['Volume Avg'] = data['volume'].rolling(window=20, min_periods=1).mean()

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
    market = "BTC/KRW"
    # 감시할 코인
    params = ["BTC/KRW","XRP/KRW","ETH/KRW","ONDO/KRW","STX/KRW","SOL/KRW","SUI/KRW","XLM/KRW","HBAR/KRW","ADA/KRW","LINK/KRW"]
    # params = ["SOL/KRW"]
    timeframe_15m = "15m"  # 15분봉 데이터
    timeframe_1d = "1d"    # 일봉 데이터
    timezone = pytz.timezone('Asia/Seoul')
    end_time = datetime.now(timezone)

    try:

        for i in params:
            # 15분봉 데이터 가져오기
            ohlcv_15m = exchange.fetch_ohlcv(i, timeframe=timeframe_15m, limit=200)
            df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 고점/저점 계산, 추세 판단, 이동평균선 및 거래량 급등 계산
            df_15m = calculate_peaks_and_troughs(df_15m)
            df_15m = determine_trends(df_15m)
            df_15m = calculate_indicators(df_15m)

            # "50일 전"의 날짜 구하기
            days_before_50 = end_time - timedelta(days=50)
            start_of_period = timezone.localize(datetime(days_before_50.year, days_before_50.month, days_before_50.day, 0, 0, 0))

            # 전일까지의 시점 계산
            end_of_yesterday = datetime(end_time.year, end_time.month, end_time.day, tzinfo=timezone) - timedelta(seconds=1)

            # 50일부터 전일까지의 일봉 데이터 가져오기
            since = int(start_of_period.timestamp() * 1000)
            ohlcv_1d = exchange.fetch_ohlcv(i, timeframe=timeframe_1d, since=since)
            df_1d = pd.DataFrame(ohlcv_1d, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_1d['timestamp'] = pd.to_datetime(df_1d['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 50일부터 전일까지의 데이터 필터링 (혹시 불필요한 데이터가 포함될 경우 대비)
            df_1d = df_1d[(df_1d['timestamp'] >= start_of_period) & (df_1d['timestamp'] <= end_of_yesterday)]

            # 결과 출력
            print(f"{i} 분석 종료 시간: {end_time}")

            one_hour_ago = end_time - timedelta(hours=1)

            for _, row_15m in df_15m.iterrows():
                timestamp = row_15m['timestamp']
                close_m = row_15m['close']
                trend = row_15m['Trend']
                volume_surge = row_15m['Volume Surge']
                ma_200 = row_15m['200MA']
                resistance1 = row_15m['resistance1']
                resistance2 = row_15m['resistance2']
                support1 = row_15m['support1']
                support2 = row_15m['support2']

                # if timestamp >= one_hour_ago:
                if volume_surge and trend == 'Uptrend' and close_m < ma_200:
                    # if close_m <= support1:
                    #     rsi = calculate_rsi(df_1d)
                    #     message = f"{i} 피봇 과매도 매수 신호 발생 시간: {timestamp}, 가격: {close_m}, Support1: {support1}, ma_200: {ma_200}, RSI: {rsi}"
                    if close_m <= support2:
                        message = f"{i} 피봇 과매도 매수 신호 발생 시간: {timestamp}, 가격: {close_m}, Support2: {support2}, ma_200: {ma_200}"
                        print(message)
                        # Slack 메시지 전송
                        send_slack_message("#매매신호", message)

                elif volume_surge and trend == 'Downtrend' and close_m > ma_200:
                    # if close_m >= resistance1:
                    #     rsi = calculate_rsi(df_1d)
                    #     message = f"{i} 피봇 과매수 매도 신호 발생 시간: {timestamp}, 가격: {close_m}, resistance1: {resistance1}, ma_200: {ma_200}, RSI: {rsi}"
                    if close_m >= resistance2:
                        message = f"{i} 피봇 과매수 매도 신호 발생 시간: {timestamp}, 가격: {close_m}, resistance2: {resistance2}, ma_200: {ma_200}"
                        print(message)
                        # Slack 메시지 전송
                        send_slack_message("#매매신호", message)    

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