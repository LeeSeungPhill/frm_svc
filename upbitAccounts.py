import jwt
import hashlib
import os
import requests
import uuid
from urllib.parse import urlencode, unquote
from dotenv import load_dotenv
load_dotenv()
from decimal import Decimal, ROUND_DOWN
import time

# access_key = os.environ['UPBIT_0825_ACCESS_KEY']
# secret_key = os.environ['UPBIT_0825_SECRET_KEY']
# api_url = os.environ['UPBIT_API']
# access_key = os.getenv("UPBIT_0825_ACCESS_KEY")
# secret_key = os.getenv("UPBIT_0825_SECRET_KEY")
access_key = os.getenv("UPBIT_77_ACCESS_KEY")
secret_key = os.getenv("UPBIT_77_SECRET_KEY")
api_url = os.getenv("UPBIT_API")

payload = {
    'access_key': access_key,
    'nonce': str(uuid.uuid4()),
}

jwt_token = jwt.encode(payload, secret_key)
authorization = 'Bearer {}'.format(jwt_token)
headers = {
  'Authorization': authorization,
}

accounts = requests.get(api_url + '/v1/accounts',headers=headers).json()

for item in accounts:
  name = item['currency']
  price = float(item['avg_buy_price'])      # 평균단가    
  volume = float(item['balance'])           # 보유수량
  amt = int(price * volume)                 # 보유금액 
    
  if item['currency'] != "KRW":
    params = {
        "markets": "KRW-"+item['currency']
    }

    trade_price = 0
    current_amt = 0
    loss_profit_amt = 0
    loss_profit_rate = 0
    # 현재가 정보
    res = requests.get(api_url + "/v1/ticker", params=params).json()

    if isinstance(res, dict) and 'error' in res:
        # 에러 메시지가 반환된 경우
        error_name = res['error'].get('name', 'Unknown')
        error_message = res['error'].get('message', 'Unknown')
        # print(f"Error {error_name}: {error_message}")
        # print(item['currency'])

    else:
        trade_price = float(res[0]['trade_price'])
        
        if trade_price == 0:
           continue

        # 현재평가금액
        current_amt = int(trade_price * volume)
        # 손실수익금
        loss_profit_amt = current_amt - amt
        # 손실수익률
        loss_profit_rate = ((100 - Decimal(trade_price / price) * 100) * -1).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

        print(name,"price : ",price,", volume : ",volume,", amt : ",amt,", trade_price : ",trade_price,", current_amt : ",current_amt,", loss_profit_amt : ",loss_profit_amt,", loss_profit_rate : ",loss_profit_rate)    
    
    time.sleep(0.1)
    
  else:
    print(name,"-",volume)   
  