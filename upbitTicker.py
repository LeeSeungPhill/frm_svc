import requests
from datetime import datetime, timedelta
import time

today = datetime.now()
yesterday = (datetime.now() - timedelta(days=1)).date()

datetime_with_time = datetime.combine(today, datetime.strptime('00:00:00', '%H:%M:%S').time())
# datetime_with_time = datetime.combine(yesterday, datetime.strptime('00:00:00', '%H:%M:%S').time())
start_dt = datetime_with_time.isoformat() + "+09:00"

server_url = "https://api.upbit.com"

params = {
    "markets": "KRW-BTC,KRW-XRP,KRW-ETH,KRW-GAS,KRW-ATOM,KRW-STX,KRW-SOL,KRW-SUI,KRW-ZETA,KRW-HBAR,KRW-ONDO,KRW-LINK"
}

def candle_info(market):

    # 현재일 기준 전일봉 1개를 요청
    url = "https://api.upbit.com/v1/candles/days"
    params = {  
        'market': market,  
        'count': 1,
        'to': start_dt
    }  
    headers = {"accept": "application/json"}

    response = requests.get(url, params=params, headers=headers).json()
    
    candle_list = list()

    for item in response:
        name = item['market']
        trade_price = float(item['trade_price'])
        low_price = float(item['low_price'])
        high_price = float(item['high_price'])
        trade_volume = float(item['candle_acc_trade_volume'])
        print(yesterday, "name : ",name,", trade_price : ",trade_price,", low_price : ",low_price,", high_price : ",high_price, ",  volume : ",trade_volume)
        candle_param = {
            "name" : name,
            "trade_price" : trade_price,
            "high_price" : high_price,
            "low_price" : low_price,
            "trade_volume" : trade_volume,
        }
        candle_list.append(candle_param)
    
    return candle_list

# 시세 현재가 조회
res = requests.get(server_url + "/v1/ticker", params=params).json()

for item in res:
    name = item['market']
    trade_price = float(item['trade_price'])
    low_price = float(item['low_price'])
    high_price = float(item['high_price'])
    trade_volume = float(item['acc_trade_volume'])
    print(today,"name : ",name,", trade_price : ",trade_price,", low_price : ",low_price,", high_price : ",high_price, ",  volume : ",trade_volume)
    result = candle_info(name)
    
    # 전일저가 금일종가 이탈하는 경우
    if trade_price < result[0]['low_price']:
        print("name : ",name, "현재가 : ",trade_price, "전일 저가 이탈 : ",result[0]['low_price'])
        # 거래량이 전일보다 많은 경우
        if trade_volume > result[0]['trade_volume']:
            print("name : ",name,"거래량 : ",trade_volume, "전일 거래량 : ",result[0]['trade_volume'])
    time.sleep(0.1)