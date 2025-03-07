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
import jwt
import uuid
from decimal import Decimal, ROUND_DOWN
import requests
import hashlib
from urllib.parse import urlencode, unquote

# 업비트 API 키 설정
upbit_api_url = os.environ['UPBIT_API']
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
        
# 데이터 가져오기 재시도 함수(재시도 횟수 : 3회)
def fetch_ohlcv_with_retry(exchange, symbol, timeframe_15m, limit=200, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            time.sleep(1)
            return exchange.fetch_ohlcv(symbol, timeframe=timeframe_15m, limit=limit)
        except Exception as e:
            print(f"Error fetching OHLCV data: {e}")
            retries += 1
            time.sleep(1)  # 재시도 전 1초 대기
    
    print("Max retries reached. Failed to fetch OHLCV data.")
    return None  # 실패 시 None 반환        

# 주문 전송
def place_order(access_key, secret_key, market, side, volume, price, ord_type="limit"):
    params= {
        'market': market,       # 마켓 ID
        'side': side,           # bid : 매수, ask : 매도
        'ord_type': ord_type,   # limit : 지정가 주문, price : 시장가 매수, market : 시장가 매도, best : 최유리 주문
        'price': price,         # 호가 매수
        'volume': volume        # 주문량
    }
    print(params)
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
    'Authorization': authorization,
    }

    # 주문 전송
    res = requests.post(upbit_api_url + '/v1/orders', json=params, headers=headers)
    print("result : ", res.json())
    return res.json()

# 주문 조회
def get_order(access_key, secret_key, order_uuid):
    params = {"uuid": order_uuid}
    print("order_uuid : ",order_uuid)
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
        'Authorization': authorization,
    }
    # 주문 조회
    response = requests.get(upbit_api_url + "/v1/order", params=params, headers=headers)
    print("response : ", response.json())
    return response.json()

# 매수등록 처리
def division_buy(trade_mng, conn):
    try:
        user_id = "FRM_AUTO"
        
        cur01 = conn.cursor()
        cur02 = conn.cursor()
        
        # 고객명에 의한 고객정보 조회
        query1 = "SELECT cust_num, cust_nm, market_name, acct_no, access_key, secret_key, access_token, token_publ_date FROM cust_mng WHERE cust_nm = %s AND market_name = %s"
        cur01.execute(query1, (trade_mng['cust_nm'], trade_mng['market_name']))  
        result_01 = cur01.fetchall()
        
        cust_info = {}
        if result_01:
            for idx, result in enumerate(result_01):
                cust_info = {
                                "cust_num": result[0],
                                "access_key": result[4],
                                "secret_key": result[5]
                            }  
        
        # 잔고정보 조회
        query2 = "SELECT last_order_no, last_buy_count, hold_price FROM balance_info WHERE cust_num = %s AND market_name = %s AND prd_nm = %s AND proc_yn = 'Y'"
        cur02.execute(query2, (cust_info['cust_num'], trade_mng['market_name'], trade_mng['prd_nm']))  
        result_02 = cur02.fetchall()
        
        trade_info = {}
        if result_02:
            for idx, result in enumerate(result_02):
                trade_info = {
                                "order_no": result[0],
                                "count": result[1] if result[1] is not None else 1,
                                "hold_price": result[2]
                            }
        
        buy_price = 0
        if len(trade_info) > 0:
            buy_count = int(trade_info['count'])+1
            # 보유가가 주문가보다 큰 경우 -> 매수가 = 주문가
            if int(trade_info['hold_price']) > int(trade_mng['ord_price']):
                buy_price = trade_mng['ord_price']
        else:
            buy_count = 1
            buy_price = trade_mng['ord_price']
        
        ord_no = ""
        ord_state = ""
                            
        # 매수가 존재하는 경우 주문전송 및 주문관리정보 생성
        if buy_price > 0:
            # access key
            access_key = cust_info['access_key']
            # secret_key
            secret_key = cust_info['secret_key']

            # 수수료를 제외한 최대매수 금액(1,000,000원 설정)
            # buy_division_amt_except_fee = (1000000 * Decimal('0.9995')).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

            # 매수물량
            # buy_vol = (buy_division_amt_except_fee / Decimal(buy_price)).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
            # 손절금액(50,000원 설정)
            loss_amt = 50000
            buy_vol = loss_amt / (Decimal(buy_price) - trade_mng['cut_price'])
            buy_division_amt_except_fee = (int(Decimal(buy_price) * buy_vol) * Decimal('0.9995')).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
            # 손절금액
            cut_amt = int(buy_division_amt_except_fee * (100 - (trade_mng['cut_price'] / Decimal(buy_price)) * 100) / 100)
            # 손절율
            cut_rate = (100 - (trade_mng['cut_price'] / Decimal(buy_price)) * 100).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            # 목표금액
            goal_amt = int(buy_vol * trade_mng['goal_price']) - buy_division_amt_except_fee
            # 목표율
            goal_rate = ((100 - (trade_mng['goal_price'] / Decimal(buy_price)) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            # 안전마진수량
            margin_vol = (cut_amt / (trade_mng['goal_price'] - trade_mng['cut_price'])).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

            if buy_division_amt_except_fee > 5000: # 수수료를 제외한 잔고가 5000보다 큰 경우
                print("order available balance : ",buy_division_amt_except_fee)
                print("order volume : ",buy_vol)

                if trade_mng['market_name'] == 'UPBIT':
                    # order_response = place_order(
                    #     access_key, 
                    #     secret_key, 
                    #     market=trade_mng['prd_nm'],
                    #     side="bid",                         # 매수
                    #     volume=str(buy_vol),                # 주문량
                    #     price=str(buy_price),               # 주문가
                    #     ord_type="limit"                    # 지정가 주문
                    # )
                    
                    # print("주문 응답:", order_response)
                    order_response = {"uuid": "019c715a-9925-4062-800f-86617b9b35fd"}

                    if "uuid" in order_response:
                        ord_no  = order_response["uuid"]  # 주문 ID
                        order_status = get_order(access_key, secret_key, ord_no)
                        ord_state = order_status['state']
                        print("주문 상태:", order_status)

                        # 주문관리정보 생성
                        cur2 = conn.cursor()
                        ins_param1 = (
                            cust_info['cust_num'], 
                            trade_mng['market_name'], 
                            datetime.fromisoformat(order_status['created_at']).strftime("%Y%m%d%H%M%S"), 
                            ord_no, 
                            trade_mng['prd_nm'],
                            '01',
                            order_status['state'],
                            buy_count,
                            Decimal(buy_price),
                            buy_vol,
                            int(Decimal(buy_price) * buy_vol),
                            trade_mng['cut_price'],
                            cut_rate,
                            cut_amt,
                            trade_mng['goal_price'],
                            goal_rate,
                            int(goal_amt),
                            margin_vol,
                            Decimal(order_status['executed_volume']),
                            Decimal(order_status['remaining_volume']),
                            user_id,
                            datetime.now(),
                            user_id,
                            datetime.now()
                        )
                        
                        insert1 = """INSERT INTO trade_mng (
                                        cust_num, 
                                        market_name, 
                                        ord_dtm, 
                                        ord_no, 
                                        prd_nm, 
                                        ord_tp,
                                        ord_state,
                                        ord_count,
                                        ord_price,
                                        ord_vol,
                                        ord_amt,
                                        cut_price,
                                        cut_rate,
                                        cut_amt,
                                        goal_price,
                                        goal_rate,
                                        goal_amt,
                                        margin_vol,
                                        executed_vol,
                                        remaining_vol,
                                        regr_id, 
                                        reg_date, 
                                        chgr_id, 
                                        chg_date
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )"""
                        cur2.execute(insert1, ins_param1)
                        conn.commit()
                        cur2.close()

                    else:
                        print("주문 실패:", order_response)
                        
        cur01.close()
        cur02.close()
            
        return {"ord_no": ord_no, "ord_state": ord_state}
    
    except Exception as e:
        print(f"Error division_buy : {e}")

# 매도등록 처리
def division_sell(trade_mng, conn):
    try:
        user_id = "FRM_AUTO"
        
        cur01 = conn.cursor()
        cur02 = conn.cursor()
        cur03 = conn.cursor()
        
        # 고객명에 의한 고객정보 조회
        query1 = "SELECT cust_num, cust_nm, market_name, acct_no, access_key, secret_key, access_token, token_publ_date FROM cust_mng WHERE cust_nm = %s AND market_name = %s"
        cur01.execute(query1, (trade_mng['cust_nm'], trade_mng['market_name']))  
        result_01 = cur01.fetchall()
        
        cust_info = {}
        if result_01:
            for idx, result in enumerate(result_01):
                cust_info = {
                                "cust_num": result[0],
                                "access_key": result[4],
                                "secret_key": result[5]
                            }  
        
        # 잔고정보 조회
        query2 = "SELECT last_order_no, last_sell_count, hold_price, hold_volume, loss_profit_rate, target_price, loss_price FROM balance_info WHERE cust_num = %s AND market_name = %s AND prd_nm = %s AND proc_yn = 'Y'"
        cur02.execute(query2, (cust_info['cust_num'], trade_mng['market_name'], trade_mng['prd_nm']))  
        result_02 = cur02.fetchall()
        
        trade_info = {}
        if result_02:
            for idx, result in enumerate(result_02):
                trade_info = {
                                "order_no": result[0],
                                "count":  result[1] if result[1] is not None else 1,
                                "hold_price": result[2],
                                "hold_volume": result[3],
                                "loss_profit_rate": result[4],
                                "target_price": result[5],
                                "loss_price": result[6]
                            }
                
        sell_price = 0
        if len(trade_info) > 0:
            sell_count = int(trade_info['count'])+1
            # 보유가가 주문가보다 작은 경우 -> 매도가 = 주문가
            # if int(trade_info['hold_price']) < int(trade_mng['ord_price']):
            #     sell_price = trade_mng['ord_price']
            sell_price = trade_mng['ord_price']
            sell_vol = trade_info['hold_volume']
        else:
            sell_count = 1
            
            # 주문정보 조회
            query3 = "SELECT ord_price, ord_vol, cut_price, goal_price FROM trade_mng WHERE cust_num = %s AND market_name = %s AND prd_nm = %s AND ord_tp = '01' AND ord_state = 'done'"
            cur03.execute(query3, (cust_info['cust_num'], trade_mng['market_name'], trade_mng['prd_nm']))  
            result_03 = cur03.fetchall()
            
            trade_mng_info = {}
            if result_03:
                for idx, result in enumerate(result_03):
                    trade_mng_info = {
                                    "ord_price": result[0],
                                    "ord_vol":  result[1],
                                    "cut_price": result[2],
                                    "goal_price": result[3]
                                }
            
                sell_price = trade_mng_info['ord_price']
                sell_vol = trade_mng_info['ord_vol']        
  
        ord_no = ""
        ord_state = ""
        
        # 매도가 존재하는 경우 주문전송 및 주문관리정보 생성
        if sell_price > 0:
            # access key
            access_key = cust_info['access_key']
            # secret_key
            secret_key = cust_info['secret_key']

            if trade_mng['market_name'] == 'UPBIT':
                # order_response = place_order(
                #     access_key, 
                #     secret_key, 
                #     market=trade_mng['prd_nm'],
                #     side="ask",                         # 매도
                #     volume=str(sell_vol),               # 매도량
                #     price=str(sell_price),              # 매도가
                #     ord_type="limit"                    # 지정가 주문
                # )
                
                # print("주문 응답:", order_response)
                order_response = {"uuid": "019c715a-9925-4062-800f-86617b9b35fd"}

                if "uuid" in order_response:
                    ord_no  = order_response["uuid"]  # 주문 ID
                    order_status = get_order(access_key, secret_key, ord_no)
                    ord_state = order_status['state']
                    print("주문 상태:", order_status)

                    # 주문관리정보 생성
                    cur2 = conn.cursor()
                    ins_param1 = (
                        cust_info['cust_num'], 
                        trade_mng['market_name'], 
                        datetime.fromisoformat(order_status['created_at']).strftime("%Y%m%d%H%M%S"), 
                        ord_no, 
                        trade_mng['prd_nm'],
                        '02',
                        order_status['state'],
                        sell_count,
                        Decimal(sell_price),
                        sell_vol,
                        int(Decimal(sell_price) * sell_vol),
                        Decimal(order_status['executed_volume']),
                        Decimal(order_status['remaining_volume']),
                        user_id,
                        datetime.now(),
                        user_id,
                        datetime.now()
                    )
                    
                    insert1 = """INSERT INTO trade_mng (
                                    cust_num, 
                                    market_name, 
                                    ord_dtm, 
                                    ord_no, 
                                    prd_nm, 
                                    ord_tp,
                                    ord_state,
                                    ord_count,
                                    ord_price,
                                    ord_vol,
                                    ord_amt,
                                    executed_vol,
                                    remaining_vol,
                                    regr_id, 
                                    reg_date, 
                                    chgr_id, 
                                    chg_date
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )"""
                    cur2.execute(insert1, ins_param1)
                    conn.commit()
                    cur2.close()

                else:
                    print("주문 실패:", order_response)                
                
        cur01.close()
        cur02.close()
        cur03.close()
        
        return {"ord_no": ord_no, "ord_state": ord_state}
    
    except Exception as e:
        print(f"Error division_sell : {e}")
                        
def analyze_data(trend_type):
    # 감시할 코인
    params = ["BTC/KRW","XRP/KRW","ETH/KRW","ONDO/KRW","STX/KRW","SOL/KRW","SUI/KRW","XLM/KRW","HBAR/KRW","ADA/KRW","LINK/KRW","RENDER/KRW"]
    timeframe_1d = "1d"    # 일봉 데이터
    timeframe_15m = "15m"  # 15분봉 데이터
    timezone = pytz.timezone('Asia/Seoul')
    end_time = datetime.now(timezone)

    try:

        for i in params:
            # 일봉 데이터 가져오기
            # ohlcv_1d = fetch_ohlcv_with_retry(exchange, i, timeframe_1d)
            # df_1d = pd.DataFrame(ohlcv_1d, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            # df_1d['timestamp'] = pd.to_datetime(df_1d['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 15분봉 데이터 가져오기
            ohlcv_15m = fetch_ohlcv_with_retry(exchange, i, timeframe_15m)
            df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

            # 고점/저점 계산, 추세 판단, 이동평균선 및 거래량 급등 계산
            df_15m = calculate_peaks_and_troughs(df_15m)
            df_15m = determine_trends(df_15m)
            df_15m = calculate_indicators(df_15m)
            
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
            
            # 신호 발생 상태 : 초기 "01"
            signal_buy = "01"
            signal_sell = "01"
            
            # 매매신호정보 조회
            query1 = "SELECT id, tr_dtm, tr_price, tr_volume, support_price, regist_price FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'B' AND tr_state = '01' order by tr_dtm desc"
            cur01.execute(query1, (i, ))  
            result_01 = cur01.fetchall()
            
            if result_01:
                for idx, result in enumerate(result_01):             
                    # 매매신호정보 첫번째 대상의 돌파가보다 현재가가 크고, 거래량보다 현재 거래량이 더 큰 경우
                    if idx == 0 and float(df_15m['close'].iloc[-1]) >= result[2] and float(df_15m['volume'].iloc[-1]) > result[3] and signal_buy == "01":
                    # if idx == 0 and signal_buy == "01":
                        formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{i} 매수 신호 발생 시간: {formatted_datetime}, 현재가: {df_15m['close'].iloc[-1]} 하락추세선 상단 돌파한 고점 {round(result[2], 1)} 을 돌파하였습니다."
                        print(message)
                        
                        # 신호 발생 상태 : 변경 "02"
                        signal_buy = "02"
                        
                        # 매수 주문 및 등록(trade_mng)
                        trade_mng = {
                                        "prd_nm": i,
                                        "ord_price": float(df_15m['close'].iloc[-1]),
                                        "cut_price": result[4],
                                        "goal_price": result[5],
                                        "cust_nm": "phills2",
                                        "market_name": "UPBIT"
                                    }
                        buy_process = division_buy(trade_mng, conn)
                        
                        if buy_process['ord_no'] != "":
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
                        
                    elif signal_buy == "02":    # 신호 발생 상태가 변경("02") 후, 나머지 대상 tr_state = '11' 변경 처리
                        
                        cur011 = conn.cursor()
                        upd_param1 = (
                            "AUTO_SIGNAL",   # chgr_id
                            datetime.now(),  # chg_date
                            result[0],       # id
                        )
                        
                        update1 = """UPDATE TR_SIGNAL_INFO SET 
                                        tr_state = '11',
                                        chgr_id = %s,
                                        chg_date = %s
                                    WHERE id = %s
                                """
                        cur011.execute(update1, upd_param1)
                        conn.commit()
                        cur011.close()                      

            # 매매신호정보 조회
            query2 = "SELECT id, tr_dtm, tr_price, tr_volume, support_price, regist_price FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = %s AND tr_tp = 'S' AND tr_state = '01' order by tr_dtm desc"
            cur02.execute(query2, (i, ))  
            result_02 = cur02.fetchall()
            
            if result_02:
                for idx, result in enumerate(result_02):    
                    # 매매신호정보 첫번째 대상의 이탈가보다 현재가가 작고, 거래량보다 현재 거래량이 더 큰 경우
                    if idx == 0 and float(df_15m['close'].iloc[-1]) <= result[2] and float(df_15m['volume'].iloc[-1]) > result[3] and signal_sell == "01":
                        formatted_datetime = datetime.strptime(result[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{i} 매도 신호 발생 시간: {formatted_datetime}, 현재가: {df_15m['close'].iloc[-1]} 상승추세선 하단 이탈한 저점 {round(result[2], 1)} 을 이탈하였습니다."
                        print(message)
                        
                        # 신호 발생 상태 : 변경 "02"
                        signal_sell = "02"
                        
                        # 매도 주문 및 등록(trade_mng)
                        trade_mng = {
                                        "prd_nm": i,
                                        "ord_price": float(df_15m['close'].iloc[-1]),
                                        "cut_price": result[4],
                                        "goal_price": result[5],
                                        "cust_nm": "phills2",
                                        "market_name": "UPBIT"
                                    }
                        sell_process = division_sell(trade_mng, conn)
                        
                        if sell_process['ord_no'] != "":
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
                        
                    elif signal_sell == "02":   # 신호 발생 상태가 변경("02") 후, 나머지 대상 tr_state = '11' 변경 처리
                        
                        cur011 = conn.cursor()
                        upd_param1 = (
                            "AUTO_SIGNAL",   # chgr_id
                            datetime.now(),  # chg_date
                            result[0],       # id
                        )
                        
                        update1 = """UPDATE TR_SIGNAL_INFO SET 
                                        tr_state = '11',
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
                trend_info = check_trend(df_15m, current_date, current_price, current_volume, prev_volume, trend_type)

                if timestamp >= one_hour_ago:

                    # 거래량 급등(거래량이 20일 거래량 평균보다 150% 이상) 인 경우 
                    if trend_info['result'] == "Turn Up" and volume_surge and trend == "Uptrend":
                        
                        # 커서 생성
                        cur1 = conn.cursor()
                        
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                        
                        # 매매신호정보 존재여부 조회
                        cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = '"+i+"' AND tr_tp = 'B' AND tr_dtm = '"+tr_dtm+"'")
                        result_one = cur1.fetchone()
                        
                        if result_one is None:
                            print("predicted_high 1 : ",trend_info["predicted_high"])
                            print("predicted_low 1 : ",trend_info["predicted_low"])
                            print("high_slope 1 : ",trend_info["high_slope"])
                            print("low_slope 1 : ",trend_info["low_slope"])
                            print("high_intercept 1 : ",trend_info["high_intercept"])
                            print("low_intercept 1 : ",trend_info["low_intercept"])
                            print("high_prices 1 : ",trend_info["high_prices"])
                            print("low_prices 1 : ",trend_info["low_prices"])
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
                                datetime.now(),  # chg_date
                                trend_info["low_prices"],
                                trend_info["high_prices"]
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
                                            chg_date,
                                            support_price,
                                            regist_price
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )"""
                            cur2.execute(insert1, ins_param1)
                            conn.commit()
                            cur2.close()
                        cur1.close()    
                        
                    elif trend_info['result'] == "Turn Down" and volume_surge and trend == "Downtrend":
                        
                        # 커서 생성
                        cur1 = conn.cursor()
                        
                        tr_dtm = timestamp.strftime('%Y%m%d%H%M%S')
                        
                        # 매매신호정보 존재여부 조회
                        cur1.execute("SELECT id FROM TR_SIGNAL_INFO WHERE signal_name = 'TrendLine-"+trend_type+"' AND prd_nm = '"+i+"' AND tr_tp = 'S' AND tr_dtm = '"+tr_dtm+"'")
                        result_one = cur1.fetchone()
                        
                        if result_one is None:
                            print("predicted_high 2 : ",trend_info["predicted_high"])
                            print("predicted_low 2 : ",trend_info["predicted_low"])
                            print("high_slope 2 : ",trend_info["high_slope"])
                            print("low_slope 2 : ",trend_info["low_slope"])
                            print("high_intercept 2 : ",trend_info["high_intercept"])
                            print("low_intercept 2 : ",trend_info["low_intercept"])
                            print("high_prices 2 : ",trend_info["high_prices"])
                            print("low_prices 2 : ",trend_info["low_prices"])
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
                                datetime.now(),  # chg_date
                                trend_info["low_prices"],
                                trend_info["high_prices"]
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
                                            chg_date,
                                            support_price,
                                            regist_price
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
schedule.every(15).minutes.do(analyze_data, 'short')        

# 실행
if __name__ == "__main__":
    print("15분마다 분석 작업을 실행합니다...")
    analyze_data('short')  # 첫 실행
    while True:
        schedule.run_pending()
        time.sleep(1)