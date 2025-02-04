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

def get_trend_line(dates, prices):
    """
    날짜와 가격 데이터를 받아 선형 회귀를 통해 추세선을 생성.
    """
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
        return result
    elif current_price < predicted_low:
        result = "Turn Down"
        return result
    else:
        result = "Normal"
        return result    

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
    timeframe_1d = "1d"    # 일봉 데이터
    timeframe_15m = "15m"  # 15분봉 데이터
    timezone = pytz.timezone('Asia/Seoul')
    end_time = datetime.now(timezone)

    try:

        for i in params:
            # 일봉 데이터 가져오기
            ohlcv_1d = exchange.fetch_ohlcv(i, timeframe=timeframe_1d, limit=200)
            df_1d = pd.DataFrame(ohlcv_1d, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_1d['timestamp'] = pd.to_datetime(df_1d['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 15분봉 데이터 가져오기
            ohlcv_15m = exchange.fetch_ohlcv(i, timeframe=timeframe_15m, limit=200)
            df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 고점/저점 계산, 추세 판단, 이동평균선 및 거래량 급등 계산
            df_15m = calculate_peaks_and_troughs(df_15m)
            df_15m = determine_trends(df_15m)
            df_15m = calculate_indicators(df_15m)

            trend_type='mid'
            
            # PostgreSQL 데이터베이스에 연결
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )

            # 커서 생성
            cur01 = conn.cursor()
            cur02 = conn.cursor()
            # 현재가 기준 매매신호정보 돌파가보다 큰 경우 조회
            query1 = "SELECT id, tr_dtm, tr_price, tr_volume FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'B' AND tr_state = '01' AND tr_price <= %s order by tr_dtm desc"
            cur01.execute(query1, (i, float(df_15m['close'].iloc[-1])))  
            result_01 = cur01.fetchall()
            for result in result_01:
                
                # 매매신호정보의 거래량보다 현재 거래량이 더 큰 경우
                if float(df_15m['volume'].iloc[-1]) > result[3]:
                    formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                    message = f"{i} 매수 신호 발생 시간: {formatted_datetime}, 하락추세선 상단을 돌파한 고점 {result[2]} 을 돌파하였습니다."
                    print(message)
                    
                    # Slack 메시지 전송
                    send_slack_message("#매매신호", message)
                    
                    cur011 = conn.cursor()
                    upd_param1 = (
                        "AUTO_SIGNAL",   # chgr_id
                        datetime.now(),  # chg_date
                        result[0],       # id
                    )
                    
                    update1 = """UPDATE TR_SIGNAL_INFO SET 
                                    tr_state = '02',
                                    chgr_id = %s,
                                    chg_date = %s
                                WHERE id = %s
                            """
                    cur011.execute(update1, upd_param1)
                    conn.commit()
                    cur011.close()

            # 현재가 기준 매매신호정보 이탈가보다 작은 경우 조회
            query2 = "SELECT id, tr_dtm, tr_price, tr_volume FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '01' AND tr_price >= %s order by tr_dtm desc"
            cur02.execute(query2, (i, float(df_15m['close'].iloc[-1])))  
            result_02 = cur02.fetchall()
            for result in result_02:
                
                # 매매신호정보의 거래량보다 현재 거래량이 더 큰 경우
                if float(df_15m['volume'].iloc[-1]) > result[3]:
                    formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                    message = f"{i} 매도 신호 발생 시간: {formatted_datetime}, 상승추세선 하단을 이탈한 저점 {result[2]} 을 이탈하였습니다."
                    print(message)
                    
                    # Slack 메시지 전송
                    send_slack_message("#매매신호", message)
                    
                    cur011 = conn.cursor()
                    upd_param1 = (
                        "AUTO_SIGNAL",   # chgr_id
                        datetime.now(),  # chg_date
                        result[0],       # id
                    )
                    
                    update1 = """UPDATE TR_SIGNAL_INFO SET 
                                    tr_state = '02',
                                    chgr_id = %s,
                                    chg_date = %s
                                WHERE id = %s
                            """
                    cur011.execute(update1, upd_param1)
                    conn.commit()
                    cur011.close()

            # 결과 출력
            print(f"{i} 분석 종료 시간: {end_time}")

            one_hour_ago = end_time - timedelta(hours=1)

            for _, row_15m in df_15m.iterrows():
                timestamp = row_15m['timestamp']
                close = row_15m['close']
                h_close = row_15m['high']
                l_close = row_15m['low']
                trend = row_15m['Trend']
                volume_surge = row_15m['Volume Surge']
                volume = row_15m['volume']
                ma_200 = row_15m['200MA']

                current_date = df_15m.iloc[-1]['timestamp']
                current_price = df_15m.iloc[-1]['close']
                current_volume = df_15m.iloc[-1]['volume']
                prev_volume = df_15m.iloc[-2]['volume']

                # result = check_trend(df_1d, current_date, current_price, current_volume, prev_volume, trend_type)
                result = check_trend(df_15m, current_date, current_price, current_volume, prev_volume, trend_type)

                if timestamp >= one_hour_ago:

                    # 거래량 급등(거래량이 20일 거래량 평균보다 150% 이상) 인 경우 
                    if result == "Turn Up" and volume_surge and trend == "Uptrend":
                        
                        # 커서 생성
                        cur1 = conn.cursor()
                        
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                        
                        # 매매신호정보 존재여부 조회
                        cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = '"+i+"' AND tr_tp = 'B' AND tr_dtm = '"+tr_dtm+"'")
                        result_one = cur1.fetchone()
                        
                        if result_one is None:
                            cur2 = conn.cursor()
                            ins_param1 = (
                                i,               # prd_nm
                                "B",             # tr_tp
                                tr_dtm,          # tr_dtm
                                "01",            # tr_state
                                h_close,         # tr_price
                                volume,          # tr_volume
                                "TrendLine-"+trend_type,    # signal_name
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
                        cur1.close()    
                        
                    elif result == "Turn Down" and volume_surge and trend == "Downtrend":
                        
                        # 커서 생성
                        cur1 = conn.cursor()
                        
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                        
                        # 매매신호정보 존재여부 조회
                        cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = '"+i+"' AND tr_tp = 'S' AND tr_dtm = '"+tr_dtm+"'")
                        result_one = cur1.fetchone()
                        
                        if result_one is None:
                            cur2 = conn.cursor()
                            ins_param1 = (
                                i,               # prd_nm
                                "S",             # tr_tp
                                tr_dtm,          # tr_dtm
                                "01",            # tr_state
                                l_close,         # tr_price
                                volume,          # tr_volume
                                "TrendLine-"+trend_type,    # signal_name
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
                        cur1.close()    
                        
            # 연결 종료
            cur01.close()
            cur02.close()
            conn.close()

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